import { NextResponse } from 'next/server'
import { getColumns } from '@/lib/db'

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url)
    const db = searchParams.get('db')

    if (!db) {
      return NextResponse.json({ error: 'Database name is required' }, { status: 400 })
    }

    const columns = await getColumns(db)
    return NextResponse.json({ columns })
  } catch (error) {
    console.error('Error fetching columns:', error)
    return NextResponse.json({ error: 'Failed to fetch columns' }, { status: 500 })
  }
} 