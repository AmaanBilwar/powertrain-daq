import { NextResponse } from 'next/server'
import { getDatabases } from '@/lib/db'

export async function GET() {
  try {
    const databases = await getDatabases()
    return NextResponse.json({ databases })
  } catch (error) {
    console.error('Error fetching databases:', error)
    return NextResponse.json({ error: 'Failed to fetch databases' }, { status: 500 })
  }
} 