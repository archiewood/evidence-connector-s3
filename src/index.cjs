const {
	EvidenceType,
	TypeFidelity,
	asyncIterableToBatchedAsyncGenerator,
	cleanQuery,
	exhaustStream
} = require('@evidence-dev/db-commons');
const runQuery = require('@evidence-dev/duckdb');
const yaml = require('yaml');
const { Database } = require('duckdb-async');

/**
 * @typedef {Object} DuckDBOptions
 * @property {string} options
 */

/**
 * Implementing this function creates an "advanced" connector
 *
 * @see https://docs.evidence.dev/plugins/create-source-plugin/
 * @type {import("@evidence-dev/db-commons").ProcessSource<DuckDBOptions>}
 */
module.exports.processSource = async function* (options, sourceFiles, utilFuncs) {
	const { accessKeyId, secretAccessKey, region } = options;
	let db, conn;

	if (!("files.yaml" in sourceFiles)) {
		throw new Error('No YAML file found in source files');
	}
	const yamlFile = await sourceFiles["files.yaml"]();
	const yamlContent = yaml.parse(yamlFile);

	const files = yamlContent.files;
	

	console.log(files);

	if (!files || files.length === 0) {
		throw new Error('No files specified in the YAML file');
	}

	db = await Database.create(':memory:', {
		access_mode: 'READ_WRITE',
		custom_user_agent: 'evidence-dev'
	});
	conn = await db.connect();

	await conn.exec(`
		CREATE SECRET my_secret (
			TYPE S3,
			KEY_ID '${accessKeyId}',
			SECRET '${secretAccessKey}',
			REGION '${region}'
		);
	`);

	try {
		for (const file of files) {
			console.log(file.name);
			console.log(file.s3_path);
			const s3path = file.s3_path;
			const name = file.name;


			const queryString = `FROM '${s3path}'`;
			
			const count_query = `WITH root as (${queryString}) SELECT COUNT(*) FROM root`;
			const expected_count = await db.all(count_query).catch(() => null);
			const expected_row_count = expected_count?.[0]['count_star()'];

			const column_query = `DESCRIBE ${queryString}`;
			const column_types = await db.all(column_query).then(duckdbDescribeToEvidenceType).catch(() => null);

			yield {
				title: name,
				content: queryString,
				rows: async function* () {
					const stream = conn.stream(queryString);
					for await (const batch of asyncIterableToBatchedAsyncGenerator(stream, 100000, {
						mapResultsToEvidenceColumnTypes: column_types == null ? mapResultsToEvidenceColumnTypes : undefined,
						standardizeRow,
						closeConnection: () => {}  // We'll close the connection after processing all files
					})) {
						yield batch;
					}
				},
				columnTypes: column_types,
				expectedRowCount: typeof expected_row_count === 'bigint' ? Number(expected_row_count) : expected_row_count
			};
		}
	} finally {
		// Close the connection after processing all files
		if (db) {
			await db.close();
		}
	}
};

/**
 * Converts BigInt values to Numbers in an object.
 * @param {Record<string, unknown>} obj - The input object with potential BigInt values.
 * @returns {Record<string, unknown>} - The object with BigInt values converted to Numbers.
 */
function standardizeRow(obj) {
	for (const key in obj) {
		if (typeof obj[key] === 'bigint') {
			obj[key] = Number(obj[key]);
		}
	}
	return obj;
}

/**
 *
 * @param {unknown} data
 * @returns {EvidenceType | undefined}
 */
function nativeTypeToEvidenceType(data) {
	switch (typeof data) {
		case 'number':
			return EvidenceType.NUMBER;
		case 'string':
			return EvidenceType.STRING;
		case 'boolean':
			return EvidenceType.BOOLEAN;
		case 'object':
			if (data instanceof Date) {
				return EvidenceType.DATE;
			}
			throw new Error(`Unsupported object type: ${data}`);
		default:
			return EvidenceType.STRING;
	}
}

/**
 * @param {Record<string, unknown>[]} rows
 * @returns {import('@evidence-dev/db-commons').ColumnDefinition[]}
 */
const mapResultsToEvidenceColumnTypes = function (rows) {
	return Object.entries(rows[0]).map(([name, value]) => {
		/** @type {TypeFidelity} */
		let typeFidelity = TypeFidelity.PRECISE;
		let evidenceType = nativeTypeToEvidenceType(value);
		if (!evidenceType) {
			typeFidelity = TypeFidelity.INFERRED;
			evidenceType = EvidenceType.STRING;
		}
		return { name, evidenceType, typeFidelity };
	});
};

/**
 * @param {{ column_name: string; column_type: string; }[]} describe
 * @returns {import('@evidence-dev/db-commons').ColumnDefinition[]}
 */
function duckdbDescribeToEvidenceType(describe) {
	return describe.map((column) => {
		let type;
		if (/DECIMAL/i.test(column.column_type)) {
			type = EvidenceType.NUMBER;
		} else {
			switch (column.column_type) {
				case 'BOOLEAN':
					type = EvidenceType.BOOLEAN;
					break;
				case 'DATE':
				case 'TIMESTAMP':
				case 'TIMESTAMP WITH TIME ZONE':
				case 'TIMESTAMP_S':
				case 'TIMESTAMP_MS':
				case 'TIMESTAMP_NS':
					type = EvidenceType.DATE;
					break;
				case 'DOUBLE':
				case 'FLOAT':
				case 'TINYINT':
				case 'UTINYINT':
				case 'SMALLINT':
				case 'USMALLINT':
				case 'INTEGER':
				case 'UINTEGER':
				case 'UBIGINT':
				case 'HUGEINT':
					type = EvidenceType.NUMBER;
					break;
				case 'BIGINT':
					type = EvidenceType.NUMBER;
					break;
				case 'DECIMAL':
				case 'TIME':
				case 'TIME WITH TIME ZONE':
					type = EvidenceType.STRING;
					break;
				default:
					type = EvidenceType.STRING;
					break;
			}
		}
		return { name: column.column_name, evidenceType: type, typeFidelity: TypeFidelity.PRECISE };
	});
}

/** @type {import("@evidence-dev/db-commons").ConnectionTester<DuckDBOptions>} */
module.exports.testConnection = async (opts) => {
	const { accessKeyId, secretAccessKey, region } = opts;
	const db = await Database.create(':memory:', {
		access_mode: 'READ_WRITE',
		custom_user_agent: 'evidence-dev'
	});
	const conn = await db.connect();

	try {
		await conn.exec(`
			CREATE SECRET my_secret (
				TYPE S3,
				KEY_ID '${accessKeyId}',
				SECRET '${secretAccessKey}',
				REGION '${region}'
			);
		`);
		await conn.all('SELECT 1');
		return true;
	} catch (e) {
		return { reason: e.message ?? 'Connection failed' };
	} finally {
		await db.close();
	}
};

module.exports.options = {
	accessKeyId: {
		title: 'Access Key ID',
		description:
			"Access Key ID for the AWS S3 bucket.",
		type: 'string',
		secret: false,
		shown: true,
		default: ''
	},
	secretAccessKey: {
		title: 'Secret Access Key',
		description:
			"Secret Access Key for the AWS S3 bucket.",
		type: 'string',
		secret: true,
		default: ''
	},
	region: {
		title: 'Region',
		description:
			"Region for the AWS S3 bucket.",
		type: 'string',
		secret: false,
		default: ''
	}
};