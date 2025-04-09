import fs from 'fs/promises';
import path from 'path';
import mysql from 'mysql2/promise';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { execSync } from 'child_process';

// Get the directory name of the current module
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration
const DEFAULT_OUTPUT_DIR = path.join(__dirname, 'generated');

/**
 * Main function to generate MySQL datasource files
 */
async function generateMySQLDatasource() {
  try {
    // Parse command line arguments
    const args = process.argv.slice(2);
    const schemaName = args[0];
    
    if (!schemaName) {
      console.error('Error: Schema name is required');
      console.log('Usage: node index-dev.js <schema_name> [output_directory] [connection_string]');
      process.exit(1);
    }
    
    const outputDir = args[1] || DEFAULT_OUTPUT_DIR;
    const connectionString = args[2] || process.env.MYSQL_CONNECTION_STRING;
    
    if (!connectionString) {
      console.error('Error: MySQL connection string is required either as an argument or as MYSQL_CONNECTION_STRING environment variable');
      process.exit(1);
    }
    
    console.log(`Generating MySQL datasource for schema: ${schemaName}`);
    console.log(`Output directory: ${outputDir}`);
    
    // Create output directory structure
    await createDirectoryStructure(outputDir);
    
    // Connect to the database
    const connection = await connectToDatabase(connectionString);
    
    // Get all tables in the schema
    const tables = await getTables(connection, schemaName);
    console.log(`Found ${tables.length} tables in schema ${schemaName}`);
    
    // Generate files for each table
    for (const table of tables) {
      console.log(`Processing table: ${table.TABLE_NAME}`);
      
      // Get table columns
      const columns = await getTableColumns(connection, schemaName, table.TABLE_NAME);
      
      // Generate CRUD files
      await generateCrudFiles(outputDir, schemaName, table.TABLE_NAME, columns);
      
      // Generate mapper file
      await generateMapperFile(outputDir, schemaName, table.TABLE_NAME, columns);
      
      // Generate SQL files
      await generateSqlFiles(outputDir, schemaName, table.TABLE_NAME, columns);
    }
    
    // Generate index file
    await generateIndexFile(outputDir, schemaName, tables);
    
    // Close database connection
    await connection.end();
    
    console.log('MySQL datasource generation completed successfully');
  } catch (error) {
    console.error('Error generating MySQL datasource:', error);
    process.exit(1);
  }
}

/**
 * Create the directory structure for the generated files
 */
async function createDirectoryStructure(outputDir) {
  const directories = [
    outputDir,
    path.join(outputDir, 'crud'),
    path.join(outputDir, 'mappers'),
    path.join(outputDir, 'sql')
  ];
  
  for (const dir of directories) {
    await fs.mkdir(dir, { recursive: true });
    console.log(`Created directory: ${dir}`);
  }
}

/**
 * Connect to the MySQL database
 */
async function connectToDatabase(connectionString) {
  try {
    // Parse connection string
    // Format: mysql://user:password@host:port/database
    const url = new URL(connectionString);
    const config = {
      host: url.hostname,
      port: url.port || 3306,
      user: url.username,
      password: url.password,
      database: url.pathname.substring(1)
    };
    
    const connection = await mysql.createConnection(config);
    console.log('Connected to MySQL database');
    return connection;
  } catch (error) {
    console.error('Error connecting to MySQL database:', error);
    throw error;
  }
}

/**
 * Get all tables in the schema
 */
async function getTables(connection, schemaName) {
  const [rows] = await connection.execute(`
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = ? 
    AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
  `, [schemaName]);
  
  return rows;
}

/**
 * Get columns for a specific table
 */
async function getTableColumns(connection, schemaName, tableName) {
  const [rows] = await connection.execute(`
    SELECT 
      COLUMN_NAME, 
      DATA_TYPE, 
      CHARACTER_MAXIMUM_LENGTH,
      IS_NULLABLE, 
      COLUMN_KEY, 
      EXTRA,
      COLUMN_DEFAULT
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = ? 
    AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
  `, [schemaName, tableName]);
  
  return rows;
}

/**
 * Generate CRUD files for a table
 */
async function generateCrudFiles(outputDir, schemaName, tableName, columns) {
  const crudDir = path.join(outputDir, 'crud');
  const fileName = `${tableName}.js`;
  const filePath = path.join(crudDir, fileName);
  
  // Find primary key column
  const primaryKey = columns.find(col => col.COLUMN_KEY === 'PRI')?.COLUMN_NAME || 'id';
  
  // Generate CRUD operations
  const content = `
import { SQLQueryBuilder } from '@ares/core/sql-query-builder.js';
import { ${toCamelCase(tableName, true)}Mapper } from '../mappers/${tableName}.js';

/**
 * CRUD operations for ${tableName} table
 */
export class ${toCamelCase(tableName, true)}CRUD {
  constructor(datasource) {
    this.datasource = datasource;
    this.mapper = new ${toCamelCase(tableName, true)}Mapper();
    this.queryBuilder = new SQLQueryBuilder('${tableName}');
  }

  /**
   * Get all records from ${tableName}
   * @param {Object} options - Query options (limit, offset, where, orderBy)
   * @returns {Promise<Array>} - Array of records
   */
  async getAll(options = {}) {
    const query = this.queryBuilder.select('*');
    
    if (options.where) {
      query.where(options.where);
    }
    
    if (options.orderBy) {
      query.orderBy(options.orderBy);
    }
    
    if (options.limit) {
      query.limit(options.limit);
      
      if (options.offset) {
        query.offset(options.offset);
      }
    }
    
    const result = await this.datasource.executeQueryAsync(query.build(), query.getParams());
    return result.results.map(row => this.mapper.toEntity(row));
  }

  /**
   * Get a record by its primary key
   * @param {*} id - Primary key value
   * @returns {Promise<Object>} - Record
   */
  async getById(id) {
    const query = this.queryBuilder
      .select('*')
      .where({ ${primaryKey}: id })
      .limit(1);
    
    const result = await this.datasource.executeQueryAsync(query.build(), query.getParams());
    return result.results.length > 0 ? this.mapper.toEntity(result.results[0]) : null;
  }

  /**
   * Create a new record
   * @param {Object} data - Record data
   * @returns {Promise<Object>} - Created record
   */
  async create(data) {
    const mappedData = this.mapper.toDatabase(data);
    const query = this.queryBuilder.insert(mappedData);
    
    const result = await this.datasource.executeQueryAsync(query.build(), query.getParams());
    
    if (result.results.insertId) {
      return this.getById(result.results.insertId);
    }
    
    return this.getById(mappedData.${primaryKey});
  }

  /**
   * Update a record
   * @param {*} id - Primary key value
   * @param {Object} data - Record data
   * @returns {Promise<Object>} - Updated record
   */
  async update(id, data) {
    const mappedData = this.mapper.toDatabase(data);
    const query = this.queryBuilder
      .update(mappedData)
      .where({ ${primaryKey}: id });
    
    await this.datasource.executeQueryAsync(query.build(), query.getParams());
    return this.getById(id);
  }

  /**
   * Delete a record
   * @param {*} id - Primary key value
   * @returns {Promise<boolean>} - Success status
   */
  async delete(id) {
    const query = this.queryBuilder
      .delete()
      .where({ ${primaryKey}: id });
    
    const result = await this.datasource.executeQueryAsync(query.build(), query.getParams());
    return result.results.affectedRows > 0;
  }

  /**
   * Count records
   * @param {Object} where - Where conditions
   * @returns {Promise<number>} - Count
   */
  async count(where = {}) {
    const query = this.queryBuilder
      .select('COUNT(*) as count')
      .where(where);
    
    const result = await this.datasource.executeQueryAsync(query.build(), query.getParams());
    return result.results[0].count;
  }
}
`;

  await fs.writeFile(filePath, content);
  console.log(`Generated CRUD file: ${filePath}`);
}

/**
 * Generate mapper file for a table
 */
async function generateMapperFile(outputDir, schemaName, tableName, columns) {
  const mappersDir = path.join(outputDir, 'mappers');
  const fileName = `${tableName}.js`;
  const filePath = path.join(mappersDir, fileName);
  
  // Generate property mappings
  const propertyMappings = columns.map(col => {
    const propName = toCamelCase(col.COLUMN_NAME);
    return `      ${propName}: row.${col.COLUMN_NAME}`;
  }).join(',\n');
  
  // Generate database mappings
  const databaseMappings = columns.map(col => {
    const propName = toCamelCase(col.COLUMN_NAME);
    return `      ${col.COLUMN_NAME}: entity.${propName}`;
  }).join(',\n');
  
  const content = `
/**
 * Mapper for ${tableName} table
 */
export class ${toCamelCase(tableName, true)}Mapper {
  /**
   * Convert database row to entity
   * @param {Object} row - Database row
   * @returns {Object} - Entity
   */
  toEntity(row) {
    if (!row) return null;
    
    return {
${propertyMappings}
    };
  }

  /**
   * Convert entity to database row
   * @param {Object} entity - Entity
   * @returns {Object} - Database row
   */
  toDatabase(entity) {
    if (!entity) return null;
    
    return {
${databaseMappings}
    };
  }
}
`;

  await fs.writeFile(filePath, content);
  console.log(`Generated mapper file: ${filePath}`);
}

/**
 * Generate SQL files for a table
 */
async function generateSqlFiles(outputDir, schemaName, tableName, columns) {
  const sqlDir = path.join(outputDir, 'sql');
  const tableDir = path.join(sqlDir, tableName);
  
  // Create table-specific directory
  await fs.mkdir(tableDir, { recursive: true });
  
  // Generate SQL files for common operations
  const operations = [
    { name: 'select-all', content: generateSelectAllSql(tableName, columns) },
    { name: 'select-by-id', content: generateSelectByIdSql(tableName, columns) },
    { name: 'insert', content: generateInsertSql(tableName, columns) },
    { name: 'update', content: generateUpdateSql(tableName, columns) },
    { name: 'delete', content: generateDeleteSql(tableName, columns) }
  ];
  
  for (const operation of operations) {
    const filePath = path.join(tableDir, `${operation.name}.sql`);
    await fs.writeFile(filePath, operation.content);
    console.log(`Generated SQL file: ${filePath}`);
  }
}

/**
 * Generate SELECT ALL SQL
 */
function generateSelectAllSql(tableName, columns) {
  const columnNames = columns.map(col => col.COLUMN_NAME).join(', ');
  
  return `-- Select all records from ${tableName}
SELECT ${columnNames}
FROM ${tableName}
/*WHERE_CLAUSE*/
/*ORDER_BY_CLAUSE*/
/*LIMIT_CLAUSE*/
/*OFFSET_CLAUSE*/`;
}

/**
 * Generate SELECT BY ID SQL
 */
function generateSelectByIdSql(tableName, columns) {
  const columnNames = columns.map(col => col.COLUMN_NAME).join(', ');
  const primaryKey = columns.find(col => col.COLUMN_KEY === 'PRI')?.COLUMN_NAME || 'id';
  
  return `-- Select a record by ID from ${tableName}
SELECT ${columnNames}
FROM ${tableName}
WHERE ${primaryKey} = :${primaryKey}
LIMIT 1`;
}

/**
 * Generate INSERT SQL
 */
function generateInsertSql(tableName, columns) {
  const columnNames = columns.map(col => col.COLUMN_NAME).join(', ');
  const placeholders = columns.map(col => `:${col.COLUMN_NAME}`).join(', ');
  
  return `-- Insert a new record into ${tableName}
INSERT INTO ${tableName} (${columnNames})
VALUES (${placeholders})`;
}

/**
 * Generate UPDATE SQL
 */
function generateUpdateSql(tableName, columns) {
  const primaryKey = columns.find(col => col.COLUMN_KEY === 'PRI')?.COLUMN_NAME || 'id';
  const setStatements = columns
    .filter(col => col.COLUMN_NAME !== primaryKey)
    .map(col => `${col.COLUMN_NAME} = :${col.COLUMN_NAME}`)
    .join(',\n  ');
  
  return `-- Update a record in ${tableName}
UPDATE ${tableName}
SET
  ${setStatements}
WHERE ${primaryKey} = :${primaryKey}`;
}

/**
 * Generate DELETE SQL
 */
function generateDeleteSql(tableName, columns) {
  const primaryKey = columns.find(col => col.COLUMN_KEY === 'PRI')?.COLUMN_NAME || 'id';
  
  return `-- Delete a record from ${tableName}
DELETE FROM ${tableName}
WHERE ${primaryKey} = :${primaryKey}`;
}

/**
 * Generate index file
 */
async function generateIndexFile(outputDir, schemaName, tables) {
  const filePath = path.join(outputDir, 'index.js');
  
  // Generate imports
  const imports = tables.map(table => {
    const className = `${toCamelCase(table.TABLE_NAME, true)}CRUD`;
    return `import { ${className} } from './crud/${table.TABLE_NAME}.js';`;
  }).join('\n');
  
  // Generate crud instances
  const crudInstances = tables.map(table => {
    const propName = `${toCamelCase(table.TABLE_NAME)}CRUD`;
    const className = `${toCamelCase(table.TABLE_NAME, true)}CRUD`;
    return `    this.${propName} = new ${className}(this.datasource);`;
  }).join('\n');
  
  const content = `
import { MySQLConnection } from '@ares/datasource-mysql';
${imports}

/**
 * ${schemaName} Database Service
 * Auto-generated datasource for ${schemaName} schema
 */
export class ${toCamelCase(schemaName, true)}Service {
  constructor(connectionParameters) {
    this.datasource = new MySQLConnection(connectionParameters);
${crudInstances}
  }

  /**
   * Connect to the database
   * @returns {Promise<void>}
   */
  async connect() {
    return new Promise((resolve, reject) => {
      this.datasource.connect((err, connection) => {
        if (err) {
          reject(err);
        } else {
          resolve(connection);
        }
      });
    });
  }

  /**
   * Disconnect from the database
   * @returns {Promise<void>}
   */
  async disconnect() {
    return this.datasource.disconnect();
  }

  /**
   * Start a transaction
   * @param {string} name - Transaction name
   * @returns {Promise<void>}
   */
  async startTransaction(name) {
    return this.datasource.startTransaction(name);
  }

  /**
   * Commit a transaction
   * @param {string} name - Transaction name
   * @returns {Promise<void>}
   */
  async commit(name) {
    return this.datasource.commit(name);
  }

  /**
   * Rollback a transaction
   * @param {string} name - Transaction name
   * @returns {Promise<void>}
   */
  async rollback(name) {
    return this.datasource.rollback(name);
  }
}
`;

  await fs.writeFile(filePath, content);
  console.log(`Generated index file: ${filePath}`);
}

/**
 * Convert a string to camelCase
 * @param {string} str - String to convert
 * @param {boolean} pascalCase - Whether to convert to PascalCase
 * @returns {string} - Converted string
 */
function toCamelCase(str, pascalCase = false) {
  // Replace non-alphanumeric characters with spaces
  let result = str.replace(/[^a-zA-Z0-9]+/g, ' ');
  
  // Convert to camelCase
  result = result
    .split(' ')
    .map((word, index) => {
      if (index === 0 && !pascalCase) {
        return word.toLowerCase();
      }
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    })
    .join('');
  
  return result;
}

/**
 * Get MySQL data type equivalent in JavaScript
 * @param {string} mysqlType - MySQL data type
 * @returns {string} - JavaScript data type
 */
function getJavaScriptType(mysqlType) {
  const typeMap = {
    'int': 'number',
    'tinyint': 'number',
    'smallint': 'number',
    'mediumint': 'number',
    'bigint': 'number',
    'float': 'number',
    'double': 'number',
    'decimal': 'number',
    'varchar': 'string',
    'char': 'string',
    'text': 'string',
    'tinytext': 'string',
    'mediumtext': 'string',
    'longtext': 'string',
    'date': 'Date',
    'datetime': 'Date',
    'timestamp': 'Date',
    'time': 'string',
    'year': 'number',
    'boolean': 'boolean',
    'json': 'Object',
    'blob': 'Buffer',
    'tinyblob': 'Buffer',
    'mediumblob': 'Buffer',
    'longblob': 'Buffer'
  };
  
  return typeMap[mysqlType.toLowerCase()] || 'any';
}

/**
 * Generate a CLI command
 */
function generateCliCommand() {
  // Create a CLI script
  const cliPath = path.join(__dirname, 'generate-datasource.js');
  const cliContent = `#!/usr/bin/env node
import { generateMySQLDatasource } from './index-dev.js';

// Run the generator
generateMySQLDatasource();
`;

  fs.writeFile(cliPath, cliContent)
    .then(() => {
      // Make the CLI script executable on Unix-like systems
      try {
        execSync(`chmod +x ${cliPath}`);
      } catch (error) {
        // Ignore errors on Windows
      }
      console.log(`Generated CLI script: ${cliPath}`);
    })
    .catch(error => {
      console.error('Error generating CLI script:', error);
    });
}

// Export the main function
export { generateMySQLDatasource };

// Run the generator if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  generateMySQLDatasource();
  generateCliCommand();
}