const {
	EvidenceType,
	TypeFidelity,
	asyncIterableToBatchedAsyncGenerator,
	exhaustStream
} = require('@evidence-dev/db-commons');
const runQuery = require('@evidence-dev/duckdb');
const fs = require('fs').promises;
const { Database } = require('duckdb-async');

/**
 * @typedef {Object} DuckDBOptions
 * @property {string} options
 */

/** @type {import("@evidence-dev/db-commons").RunQuery<DuckDBOptions>} */
module.exports = async (queryString, _, batchSize = 100000) => runQuery(queryString, { filename: ':memory:' }, batchSize);

/** @type {import("@evidence-dev/db-commons").GetRunner<DuckDBOptions>} */
module.exports.getRunner = ({ accessKeyId, secretAccessKey, region }) => {
	let db, conn;

	return async (queryContent, queryPath, batchSize = 100000) => {
		if (!queryPath.endsWith('.sql')) return null;

		if (!db) {
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
		}

		const cleanQuery = (query) => `(${query})`;

		const queryString = await fs.readFile(queryPath, 'utf8');
		const stream = conn.stream(queryString);

		const count_query = `WITH root as ${cleanQuery(queryString)} SELECT COUNT(*) FROM root`;
		const expected_count = await db.all(count_query).catch((error) => {
			console.error('Error executing count query:', error);
			return null;
		});
		const expected_row_count = expected_count?.[0]['count_star()'];

		const column_query = `DESCRIBE ${cleanQuery(queryString)}`;
		const column_types = await db.all(column_query)
			.then(duckdbDescribeToEvidenceType)
			.catch((error) => {
				console.error('Error executing describe query:', error);
				return null;
			});

		const results = await asyncIterableToBatchedAsyncGenerator(stream, batchSize, {
			mapResultsToEvidenceColumnTypes: column_types == null ? mapResultsToEvidenceColumnTypes : undefined,
			standardizeRow
		});

		if (column_types != null) {
			results.columnTypes = column_types;
		}
		results.expectedRowCount = Number(expected_row_count);

		return results;
	};
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
	const r = await runQuery('SELECT 1;', { ...opts, filename: ':memory:' })
		.then(exhaustStream)
		.then(() => true)
		.catch((e) => ({ reason: e.message ?? 'File not found' }));
	return r;
};

module.exports.options = {
	accessKeyId: {
		title: 'Access Key ID',
		type: 'string',
		secret: false,
		shown: true,
		default: ''
	},
	secretAccessKey: {
		title: 'Secret Access Key',
		type: 'string',
		secret: true,
		default: ''
	},
	region: {
		title: 'Region',
		type: 'string',
		secret: false,
		default: ''
	}
};