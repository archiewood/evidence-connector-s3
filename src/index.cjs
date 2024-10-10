const {
	EvidenceType,
	TypeFidelity,
	asyncIterableToBatchedAsyncGenerator,
	cleanQuery,
	exhaustStream
} = require('@evidence-dev/db-commons');
const runQuery = require('@evidence-dev/duckdb');
const yaml = require('yaml');
const fs = require('fs').promises;
const { Database } = require('duckdb-async');
const path = require('path');

/**
 * @typedef {Object} DuckDBOptions
 * @property {string} options
 */

/** @type {import("@evidence-dev/db-commons").RunQuery<DuckDBOptions>} */
module.exports = async (queryString, _, batchSize = 100000) => {
	return runQuery(queryString, { filename: ':memory:' }, batchSize);
};

/** @type {import("@evidence-dev/db-commons").GetRunner<DuckDBOptions>} */
module.exports.getRunner = ({ accessKeyId, secretAccessKey, region }) => {
	console.log(region);
	let db, conn;

	return async (queryContent, queryPath, batchSize = 100000) => {
		// Filter out non-yaml files
		if (!queryPath.endsWith('.yaml')) return null;
		
		// Read yaml file and get paths
		const yamlFile = await fs.readFile(queryPath, 'utf8');
		const yamlObj = yaml.parse(yamlFile);
		const files = yamlObj.files;

		const path = files[0].path;
		const name = files[0].name;

		if (!db) {
			// Create a new DuckDB connection if it doesn't exist
			db = await Database.create(':memory:', {
				access_mode: 'READ_WRITE',
				custom_user_agent: 'evidence-dev'
			});
			conn = await db.connect();

			// Create the secret
			await conn.exec(`
				CREATE SECRET my_secret (
					TYPE S3,
					KEY_ID '${accessKeyId}',
					SECRET '${secretAccessKey}',
					REGION '${region}'
				);
			`);
		}

		const cleanQuery = (query) => `(${query})`;

		const queryString = `FROM '${path}'`;
		const stream = conn.stream(queryString);

		const count_query = `WITH root as ${cleanQuery(queryString)} SELECT COUNT(*) FROM root`;
		const expected_count = await db.all(count_query).catch(() => null);
		const expected_row_count = expected_count?.[0]['count_star()'];

		const column_query = `DESCRIBE ${cleanQuery(queryString)}`;
		const column_types = await db.all(column_query).then(duckdbDescribeToEvidenceType).catch(() => null);

		const results = await asyncIterableToBatchedAsyncGenerator(stream, batchSize, {
			mapResultsToEvidenceColumnTypes:
			column_types == null ? mapResultsToEvidenceColumnTypes : undefined,
		standardizeRow,
		closeConnection: () => db.close()
		});

		if (column_types != null) {
			results.columnTypes = column_types;
		}
		results.expectedRowCount = expected_row_count;
		if (typeof results.expectedRowCount === 'bigint') {
			results.expectedRowCount = Number(results.expectedRowCount);
		}

		return results;
	};
};


/**
 * Implementing this function creates an "advanced" connector
 *
 *
 * @see https://docs.evidence.dev/plugins/create-source-plugin/
 * @type {import("@evidence-dev/db-commons").GetRunner<ConnectorOptions>}
 */
// Uncomment to use the advanced source interface
// This uses the `yield` keyword, and returns the same type as getRunner, but with an added `name` and `content` field (content is used for caching)
// sourceFiles provides an easy way to read the source directory to check for / iterate through files
// /** @type {import("@evidence-dev/db-commons").ProcessSource<ConnectorOptions>} */
// export async function* processSource(options, sourceFiles, utilFuncs) {
//   yield {
//     title: "some_demo_table",
//     content: "SELECT * FROM some_demo_table", // This is ONLY used for caching
//     rows: [], // rows can be an array
//     columnTypes: [
//       {
//         name: "someInt",
//         evidenceType: EvidenceType.NUMBER,
//         typeFidelity: "inferred",
//       },
//     ],
//   };
//   yield {
//     title: "some_demo_table",
//     content: "SELECT * FROM some_demo_table", // This is ONLY used for caching
//     rows: async function* () {}, // rows can be a generator function for returning batches of results (e.g. if an API is paginated, or database supports cursors)
//     columnTypes: [
//       {
//         name: "someInt",
//         evidenceType: EvidenceType.NUMBER,
//         typeFidelity: "inferred",
//       },
//     ],
//   };

//  throw new Error("Process Source has not yet been implemented");
// }


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
	const r = await runQuery('SELECT 1;', { ...opts, filename: ':memory:' })
		.then(exhaustStream)
		.then(() => true)
		.catch((e) => ({ reason: e.message ?? 'File not found' }));
	return r;
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
