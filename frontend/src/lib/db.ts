import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import path from 'path'

// Initialize database connection
async function getDbConnection(dbName: string) {
  const dbPath = path.join(process.cwd(), 'data', `${dbName}.db`)
  return open({
    filename: dbPath,
    driver: sqlite3.Database
  })
}

// Get list of available databases
export async function getDatabases(): Promise<string[]> {
  const dataDir = path.join(process.cwd(), 'data')
  const fs = require('fs')
  const files = fs.readdirSync(dataDir)
  return files
    .filter((file: string) => file.endsWith('.db'))
    .map((file: string) => file.replace('.db', ''))
}

// Get columns from a specific database
export async function getColumns(dbName: string): Promise<string[]> {
  const db = await getDbConnection(dbName)
  const tables = await db.all("SELECT name FROM sqlite_master WHERE type='table'")
  
  if (tables.length === 0) {
    return []
  }

  const firstTable = tables[0].name
  const columns = await db.all(`PRAGMA table_info(${firstTable})`)
  return columns.map((col: any) => col.name)
}

// Get data for visualization
export async function getData(dbName: string, xAxis: string, yAxis: string): Promise<any[]> {
  const db = await getDbConnection(dbName)
  const tables = await db.all("SELECT name FROM sqlite_master WHERE type='table'")
  
  if (tables.length === 0) {
    return []
  }

  const firstTable = tables[0].name
  return db.all(`SELECT ${xAxis}, ${yAxis} FROM ${firstTable} ORDER BY ${xAxis}`)
} 