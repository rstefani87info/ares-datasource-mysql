# @ares/datasource-mysql Documentation

## Purpose

Mysql and MariaDB module of aReS framework.

## Installation

```bash
yarn add @ares/datasource-mysql
```

In a Yarn Workspaces monorepo:

```bash
yarn workspace <app> add @ares/datasource-mysql
```

## Quickstart

This module provides integrations/drivers for the `core` datasource runtime.

Typical (conceptual) usage of a connection class:

```js
import { aReSInitialize } from "@ares/core";
import { MariaDB } from "@ares/datasource-mysql";

const aReS = aReSInitialize({ name: "my-app", environments: [{ selected: true, type: "development" }] });

// In an aReS datasource, the connection class is usually instantiated by the datasource runtime based on config.
```

## Public API (exports)

This section documents the actual public surface at entrypoint level and main exported symbols.

Root entrypoint:

- `@ares/datasource-mysql`

Main files at package root (indicative):

- `index-dev.js`
- `index.js`

Exports detected in `index.*`:

- `MariaDB`
- `MARIADBDataTypeDescriptors`

## Configuration (appSetup / config / policies)

This module is typically used inside an aReS datasource. Actual keys depend on the datasource definition and the `@ares/core` runtime.

Practical guidelines:

- define environments and select production/development via `aReS.isProduction`
- keep secrets in `config` or environment variables (never hard-coded)

## Test

Run module tests (if present):

```bash
yarn workspace @ares/datasource-mysql test
```

## Notes

- This document is maintained alongside the module tickets.

## Appendix (previous content)

## Description

The `@ares/datasource-mysql` module provides an advanced interface for connecting to and managing MySQL and MariaDB databases within the aReS framework.

## Installation

```bash
npm install @ares/datasource-mysql
```

## Key Features

### MariaDB Class
Extends `SQLDBConnection` from aReS core to provide MySQL/MariaDB-specific functionality:

- Connection pool management
- Native asynchronous connections
- Automatic session management
- Multiple statement support
- Advanced logging
- Integrated error handling

### Main Functionality

#### Constructor
```javascript
new MariaDB(connectionParameters, datasource, sessionId, connectionSettingName)
```

**Parameters:**
- `connectionParameters` (object) - Database connection parameters
- `datasource` (object) - aReS framework datasource object
- `sessionId` (string) - Unique session ID
- `connectionSettingName` (string) - Connection configuration name

#### Main Methods

##### `createPool()`
Creates and manages a database connection pool.

**Returns:** Promise that resolves to the connection pool

##### `nativeConnect(callback)`
Establishes a native connection to the MySQL/MariaDB database.

**Parameters:**
- `callback` (function) - Callback function to handle the connection

**Returns:** Promise that resolves to the connection

## Usage

### Basic Configuration

```javascript
import { MariaDB } from '@ares/datasource-mysql';

const connectionParams = {
    host: 'localhost',
    port: 3306,
    user: 'username',
    password: 'password',
    database: 'mydatabase',
    charset: 'utf8mb4'
};

const datasource = /* aReS datasource object */;
const sessionId = 'unique-session-id';
const connectionName = 'main-db';

const mariaDB = new MariaDB(
    connectionParams, 
    datasource, 
    sessionId, 
    connectionName
);
```

### Creating Connection Pool

```javascript
try {
    const pool = await mariaDB.createPool();
    console.log('Connection pool created successfully');
} catch (error) {
    console.error('Error creating pool:', error.message);
}
```

### Connecting to Database

```javascript
try {
    await mariaDB.nativeConnect((error) => {
        if (error) {
            console.error('Connection error:', error);
        } else {
            console.log('Connected to database');
        }
    });
} catch (error) {
    console.error('Error during connection:', error.message);
}
```

### Complete Example

```javascript
import { MariaDB } from '@ares/datasource-mysql';
import aReS from '@ares/core';

class DatabaseManager {
    constructor() {
        this.connections = new Map();
    }
    
    async createConnection(config) {
        const sessionId = aReS.crypto.getMD5Hash(
            `${config.host}:${config.port}:${config.database}:${Date.now()}`
        );
        
        const mariaDB = new MariaDB(
            config,
            this.datasource,
            sessionId,
            'primary'
        );
        
        try {
            // Create pool
            await mariaDB.createPool();
            
            // Establish connection
            await mariaDB.nativeConnect((error) => {
                if (error) {
                    throw new Error(`Connection failed: ${error.message}`);
                }
            });
            
            this.connections.set(sessionId, mariaDB);
            return sessionId;
            
        } catch (error) {
            console.error('Error creating connection:', error.message);
            throw error;
        }
    }
    
    getConnection(sessionId) {
        return this.connections.get(sessionId);
    }
    
    closeConnection(sessionId) {
        const connection = this.connections.get(sessionId);
        if (connection && connection.connection) {
            connection.connection.end();
            this.connections.delete(sessionId);
        }
    }
}

// Usage
const dbManager = new DatabaseManager();

const config = {
    host: 'localhost',
    port: 3306,
    user: 'myuser',
    password: 'mypassword',
    database: 'myapp',
    charset: 'utf8mb4',
    timezone: 'UTC'
};

try {
    const sessionId = await dbManager.createConnection(config);
    console.log(`Connection created with ID: ${sessionId}`);
    
    // Use the connection...
    
    // Close connection when done
    dbManager.closeConnection(sessionId);
} catch (error) {
    console.error('Error:', error.message);
}
```

## Advanced Features

### Automatic Session Management
Sessions are automatically managed and removed when the connection ends:

```javascript
// Session is automatically removed when connection closes
connection.on('end', () => {
    delete dbConn.datasource.sessions[sessionId];
});
```

### Shared Connection Pool
The system uses a shared pool to optimize resources:

```javascript
// Pool is reused if already existing for the same configuration
const pool = await this.datasource.getPool(this.connectionSettingName, () => 
    mysql.createPool({ ...this, multipleStatements: true })
);
```

### Multiple Statement Support
Automatically enabled to execute multiple queries in a single call:

```javascript
// multipleStatements: true is enabled by default
```

## Integration with aReS Core

### Usage with Datasources
```javascript
import { SQLDBConnection } from '@ares/core/datasources.js';

// MariaDB extends SQLDBConnection to inherit base functionality
```

### Advanced Logging
```javascript
import { asyncConsole } from '@ares/core/console.js';

// Uses aReS advanced logging system
```

### Data Descriptors
```javascript
import { dataDescriptors } from '@ares/core/dataDescriptors.js';

// Integration with data description system
```

## Error Handling

### Connection Errors
```javascript
try {
    await mariaDB.nativeConnect((err) => {
        if (err) {
            console.error('Error getting connection:', err);
            throw err;
        }
    });
} catch (error) {
    // Handle specific errors
    if (error.code === 'ECONNREFUSED') {
        console.error('Database server unreachable');
    } else if (error.code === 'ER_ACCESS_DENIED_ERROR') {
        console.error('Invalid access credentials');
    } else {
        console.error('Generic error:', error.message);
    }
}
```

## Advanced Configurations

### Extended Connection Parameters
```javascript
const advancedConfig = {
    host: 'localhost',
    port: 3306,
    user: 'username',
    password: 'password',
    database: 'mydatabase',
    charset: 'utf8mb4',
    timezone: 'UTC',
    acquireTimeout: 60000,
    timeout: 60000,
    reconnect: true,
    connectionLimit: 10,
    queueLimit: 0,
    ssl: {
        ca: 'path/to/ca.pem',
        cert: 'path/to/client-cert.pem',
        key: 'path/to/client-key.pem'
    }
};
```

## Dependencies

- `mysql` - MySQL driver for Node.js
- `@ares/core` - Main aReS framework

## License

MIT

## Author

Roberto Stefani

## Repository

[GitHub - ares-datasource-mysql](https://github.com/rstefani87info/ares-datasource-mysql)

## Notes

This module is designed to integrate seamlessly with the aReS ecosystem, providing a robust and scalable interface for MySQL and MariaDB databases. It's particularly useful for:

- Web applications with MySQL/MariaDB databases
- Enterprise data management systems
- Backend APIs with data persistence
- Microservices with database access
