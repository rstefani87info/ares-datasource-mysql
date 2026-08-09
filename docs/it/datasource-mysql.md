# Documentazione @ares/datasource-mysql

## Scopo

Mysql and MariaDB module of aReS framework.

## Installazione

```bash
yarn add @ares/datasource-mysql
```

In un monorepo Yarn Workspaces:

```bash
yarn workspace <app> add @ares/datasource-mysql
```

## Quickstart

Questo modulo fornisce integrazioni/driver per il runtime datasource del `core`.

Esempio tipico (concettuale) di uso di una connection class:

```js
import { aReSInitialize } from "@ares/core";
import { MariaDB } from "@ares/datasource-mysql";

const aReS = aReSInitialize({ name: "my-app", environments: [{ selected: true, type: "development" }] });

// In un datasource aReS, la connection class viene istanziata dal runtime datasource in base alla configurazione.
```

## API pubbliche (exports)

Questa sezione documenta la superficie pubblica reale a livello di entrypoint e simboli principali.

Entrypoint root:

- `@ares/datasource-mysql`

File principali nel root del package (indicativi):

- `index-dev.js`
- `index.js`

Export individuati in `index.*`:

- `MariaDB`
- `MARIADBDataTypeDescriptors`

## Configurazione (appSetup / config / policies)

Questo modulo viene tipicamente usato dentro un datasource aReS. Le chiavi effettive dipendono dalla definizione del datasource e dal runtime `@ares/core`.

Indicazioni pratiche:

- definire gli ambienti (`environments`) e selezionare production/development tramite `aReS.isProduction`
- centralizzare segreti in `config` o variabili d’ambiente (mai hard-coded)

## Test

Esecuzione test del modulo (se presenti):

```bash
yarn workspace @ares/datasource-mysql test
```

## Note

- Questo documento è mantenuto in parallelo ai ticket del modulo.

## Appendice (contenuto precedente)

## Descrizione

Il modulo `@ares/datasource-mysql` fornisce un'interfaccia avanzata per la connessione e gestione di database MySQL e MariaDB all'interno del framework aReS.

## Installazione

```bash
npm install @ares/datasource-mysql
```

## Caratteristiche Principali

### Classe MariaDB
Estende `SQLDBConnection` dal core aReS per fornire funzionalità specifiche per MySQL/MariaDB:

- Gestione pool di connessioni
- Connessioni asincrone native
- Gestione sessioni automatica
- Supporto per statement multipli
- Logging avanzato
- Gestione errori integrata

### Funzionalità Principali

#### Costruttore
```javascript
new MariaDB(connectionParameters, datasource, sessionId, connectionSettingName)
```

**Parametri:**
- `connectionParameters` (object) - Parametri di connessione al database
- `datasource` (object) - Oggetto datasource del framework aReS
- `sessionId` (string) - ID univoco della sessione
- `connectionSettingName` (string) - Nome della configurazione di connessione

#### Metodi Principali

##### `createPool()`
Crea e gestisce un pool di connessioni al database.

**Ritorna:** Promise che risolve al pool di connessioni

##### `nativeConnect(callback)`
Stabilisce una connessione nativa al database MySQL/MariaDB.

**Parametri:**
- `callback` (function) - Funzione di callback per gestire la connessione

**Ritorna:** Promise che risolve alla connessione

## Utilizzo

### Configurazione Base

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

const datasource = /* oggetto datasource aReS */;
const sessionId = 'unique-session-id';
const connectionName = 'main-db';

const mariaDB = new MariaDB(
    connectionParams, 
    datasource, 
    sessionId, 
    connectionName
);
```

### Creazione Pool di Connessioni

```javascript
try {
    const pool = await mariaDB.createPool();
    console.log('Pool di connessioni creato con successo');
} catch (error) {
    console.error('Errore nella creazione del pool:', error.message);
}
```

### Connessione al Database

```javascript
try {
    await mariaDB.nativeConnect((error) => {
        if (error) {
            console.error('Errore di connessione:', error);
        } else {
            console.log('Connesso al database');
        }
    });
} catch (error) {
    console.error('Errore durante la connessione:', error.message);
}
```

### Esempio Completo

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
            // Crea il pool
            await mariaDB.createPool();
            
            // Stabilisce la connessione
            await mariaDB.nativeConnect((error) => {
                if (error) {
                    throw new Error(`Connessione fallita: ${error.message}`);
                }
            });
            
            this.connections.set(sessionId, mariaDB);
            return sessionId;
            
        } catch (error) {
            console.error('Errore nella creazione della connessione:', error.message);
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

// Utilizzo
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
    console.log(`Connessione creata con ID: ${sessionId}`);
    
    // Utilizza la connessione...
    
    // Chiudi la connessione quando finito
    dbManager.closeConnection(sessionId);
} catch (error) {
    console.error('Errore:', error.message);
}
```

## Caratteristiche Avanzate

### Gestione Automatica Sessioni
Le sessioni vengono gestite automaticamente e rimosse quando la connessione termina:

```javascript
// La sessione viene automaticamente rimossa quando la connessione si chiude
connection.on('end', () => {
    delete dbConn.datasource.sessions[sessionId];
});
```

### Pool di Connessioni Condiviso
Il sistema utilizza un pool condiviso per ottimizzare le risorse:

```javascript
// Il pool viene riutilizzato se già esistente per la stessa configurazione
const pool = await this.datasource.getPool(this.connectionSettingName, () => 
    mysql.createPool({ ...this, multipleStatements: true })
);
```

### Supporto Statement Multipli
Abilitato automaticamente per eseguire query multiple in una singola chiamata:

```javascript
// multipleStatements: true è abilitato di default
```

## Integrazione con aReS Core

### Utilizzo con Datasources
```javascript
import { SQLDBConnection } from '@ares/core/datasources.js';

// MariaDB estende SQLDBConnection per ereditare funzionalità base
```

### Logging Avanzato
```javascript
import { asyncConsole } from '@ares/core/console.js';

// Utilizza il sistema di logging avanzato di aReS
```

### Descrittori Dati
```javascript
import { dataDescriptors } from '@ares/core/dataDescriptors.js';

// Integrazione con il sistema di descrizione dati
```

## Gestione Errori

### Errori di Connessione
```javascript
try {
    await mariaDB.nativeConnect((err) => {
        if (err) {
            console.error('Error getting connection:', err);
            throw err;
        }
    });
} catch (error) {
    // Gestisci errori specifici
    if (error.code === 'ECONNREFUSED') {
        console.error('Database server non raggiungibile');
    } else if (error.code === 'ER_ACCESS_DENIED_ERROR') {
        console.error('Credenziali di accesso non valide');
    } else {
        console.error('Errore generico:', error.message);
    }
}
```

## Configurazioni Avanzate

### Parametri di Connessione Estesi
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

## Dipendenze

- `mysql` - Driver MySQL per Node.js
- `@ares/core` - Framework principale aReS

## Licenza

MIT

## Autore

Roberto Stefani

## Repository

[GitHub - ares-datasource-mysql](https://github.com/rstefani87info/ares-datasource-mysql)

## Note

Questo modulo è progettato per integrarsi perfettamente con l'ecosistema aReS, fornendo un'interfaccia robusta e scalabile per database MySQL e MariaDB. È particolarmente utile per:

- Applicazioni web con database MySQL/MariaDB
- Sistemi di gestione dati enterprise
- API backend con persistenza dati
- Microservizi con accesso database
