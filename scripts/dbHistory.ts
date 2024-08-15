import fs from 'fs';
import path from 'path';
import sqlite3 from 'sqlite3';
import * as crypto from '@shardus/crypto-utils';
import { DBHistoryFile, AccountHistoryModel } from './types';
import { FilePaths } from '../src/shardeum/shardeumFlags';
import { Utils } from '@shardus/types';

crypto.init('69fa4195670576c0160d660c3be36556ff8d504725be8a59b5a96509e0c994bc');

let tsUpgrades = 0;
let accountsMap = new Map<string, any>();
const emptyCodeHash = 'c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470';
const dbFiles: DBHistoryFile[] = [];

const myArgs = process.argv.slice(2);
const directory = myArgs[0] || '.';

async function dbFilesFromFolders() {
  try {
    const files = await fs.promises.readdir(directory);
    for (const file of files) {
      const filepath = path.resolve(directory, file);
      if (await isDirectory(filepath)) {
        const oldFilePath = path.resolve(filepath, 'db', FilePaths.DB);
        const oldSize = await getFileSize(oldFilePath);
        console.log(`${oldFilePath} ${oldSize}`);

        const newFilepath = path.resolve(filepath, 'db', FilePaths.SHARDEUM_DB);
        const newSize = await getFileSize(newFilepath);
        console.log(`${newFilepath} ${newSize}`);

        const historyFilePath = path.resolve(filepath, 'db', FilePaths.HISTORY_DB);
        const historySize = await getFileSize(historyFilePath, true);
        console.log(`${historyFilePath} ${historySize}`);

        dbFiles.push({
          oldFilename: oldFilePath,
          newFilename: newFilepath,
          historyFileName: historyFilePath,
        });
      }
    }
  } catch (error) {
    console.error('Error reading directory:', error);
  }

  await sleep(1000);
  console.log(JSON.stringify(dbFiles, null, 2));
  await main();
}

async function main() {
  for (const dbFile of dbFiles) {
    tsUpgrades = 0;
    accountsMap.clear();

    try {
      await createHistoryDbIfNotExist(dbFile.historyFileName);
      const historyAccounts = await getHistoryAccountsFromDB(dbFile.historyFileName);
      const historyDb = getDB(dbFile.historyFileName);

      const historyAccountMap = new Map<string, AccountHistoryModel>(
        historyAccounts.map((account) => [account.accountId, account])
      );

      await loadDb(dbFile.oldFilename, true);
      await loadDb(dbFile.newFilename, false);

      for (const account of accountsMap.values()) {
        const codeHashHex = Buffer.from(account.data.account.codeHash).toString('hex');
        const updatedAccount = updateAccountHistory(account, historyAccountMap, codeHashHex);

        historyAccountMap.set(account.accountId, updatedAccount);

        const queryString = `INSERT OR REPLACE INTO accountsHistory (accountId, evmAddress, accountType, firstSeen, lastSeen, accountBalance, codehash, typeChanged) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
        await runQuery(historyDb, queryString, [
          updatedAccount.accountId,
          updatedAccount.evmAddress,
          updatedAccount.accountType,
          updatedAccount.firstSeen,
          updatedAccount.lastSeen,
          updatedAccount.accountBalance,
          updatedAccount.codehash,
          updatedAccount.typeChanged,
        ]);
      }
    } catch (error) {
      console.error('Unable to process the database:', error);
    }

    console.log(
      `Processed: ${dbFile.oldFilename}, ${dbFile.newFilename}. Accounts: ${accountsMap.size}, Upgrades: ${tsUpgrades}`
    );
  }
}

async function loadDb(filename: string, isOld: boolean) {
  const newestAccounts = await getNewestAccountsFromDB(filename, isOld);
  for (const account of newestAccounts) {
    if (account.data.accountType !== 0) continue;

    if (accountsMap.has(account.accountId)) {
      const existingAccount = accountsMap.get(account.accountId);
      if (account.timestamp > existingAccount.timestamp) {
        accountsMap.set(account.accountId, account);
        tsUpgrades++;
      }
    } else {
      accountsMap.set(account.accountId, account);
    }
  }
}

function getDB(dbPath: string) {
  return new sqlite3.Database(dbPath, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE, (err) => {
    if (err) console.error('Error opening database:', err.message);
  });
}

async function getHistoryAccountsFromDB(dbPath: string) {
  const db = getDB(dbPath);
  const queryString = `SELECT * FROM accountsHistory ORDER BY accountId ASC`;
  return await runQuery(db, queryString);
}

async function runQuery(db: sqlite3.Database, query: string, params: any[] = []) {
  return new Promise<any[]>((resolve, reject) => {
    db.all(query, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

async function getNewestAccountsFromDB(dbPath: string, isOld: boolean) {
  const db = getDB(dbPath);
  const queryString = isOld
    ? `SELECT a.accountId, a.data, a.timestamp, a.hash, a.isGlobal, a.cycleNumber FROM accountsCopy a INNER JOIN (SELECT accountId, MAX(timestamp) timestamp FROM accountsCopy GROUP BY accountId) b ON a.accountId = b.accountId AND a.timestamp = b.timestamp ORDER BY a.accountId ASC`
    : `SELECT a.accountId, a.data, a.timestamp FROM accountsEntry a INNER JOIN (SELECT accountId, MAX(timestamp) timestamp FROM accountsEntry GROUP BY accountId) b ON a.accountId = b.accountId AND a.timestamp = b.timestamp ORDER BY a.accountId ASC`;
  const accounts = await runQuery(db, queryString);
  return accounts.map((acc) => ({ ...acc, data: Utils.safeJsonParse(acc.data), isOld }));
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function createHistoryDbIfNotExist(dbPath: string) {
  if (fs.existsSync(dbPath)) {
    console.log('History DB already exists');
    return;
  }
  const db = new sqlite3.Database(dbPath);
  await db.run('PRAGMA synchronous = OFF');
  await db.run(
    `CREATE TABLE IF NOT EXISTS accountsHistory (
      accountId VARCHAR(255) NOT NULL, 
      evmAddress VARCHAR(42) NOT NULL, 
      accountType VARCHAR(3) NOT NULL, 
      firstSeen BIGINT NOT NULL, 
      lastSeen BIGINT NOT NULL, 
      accountBalance VARCHAR(255) NOT NULL, 
      codehash VARCHAR(255) NOT NULL, 
      typeChanged BOOLEAN NOT NULL, 
      PRIMARY KEY (accountId)
    )`
  );
}

async function getFileSize(filePath: string, checkExistence = false): Promise<number> {
  if (checkExistence && !fs.existsSync(filePath)) return -1;
  try {
    const stats = await fs.promises.stat(filePath);
    return stats.size;
  } catch {
    return -1;
  }
}

async function isDirectory(filePath: string): Promise<boolean> {
  try {
    const stats = await fs.promises.lstat(filePath);
    return stats.isDirectory();
  } catch {
    return false;
  }
}

function updateAccountHistory(
  account: any,
  historyAccountMap: Map<string, AccountHistoryModel>,
  codeHashHex: string
): AccountHistoryModel {
  const existingHistoryAccount = historyAccountMap.get(account.accountId);
  if (existingHistoryAccount) {
    existingHistoryAccount.lastSeen = account.data.timestamp;
    existingHistoryAccount.accountBalance = account.data.account.balance;
    if (codeHashHex !== existingHistoryAccount.codehash) {
      existingHistoryAccount.codehash = codeHashHex;
      existingHistoryAccount.typeChanged = true;
      existingHistoryAccount.accountType = codeHashHex === emptyCodeHash ? 'EOA' : 'CA';
    }
    return existingHistoryAccount;
  } else {
    return {
      accountId: account.accountId,
      evmAddress: account.data.ethAddress,
      accountType: codeHashHex === emptyCodeHash ? 'EOA' : 'CA',
      firstSeen: account.data.timestamp,
      lastSeen: account.data.timestamp,
      accountBalance: account.data.account.balance,
      codehash: codeHashHex,
      typeChanged: false,
    };
  }
}

dbFilesFromFolders();
