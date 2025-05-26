import { NextResponse } from 'next/server'
import { getData } from '@/lib/db'

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url)
    const db = searchParams.get('db')
    const x = searchParams.get('x')
    const y = searchParams.get('y')

    if (!db || !x || !y) {
      return NextResponse.json(
        { error: 'Database name, x-axis, and y-axis are required' },
        { status: 400 }
      )
    }

    const data = await getData(db, x, y)
    return NextResponse.json(data)
  } catch (error) {
    console.error('Error fetching data:', error)
    return NextResponse.json({ error: 'Failed to fetch data' }, { status: 500 })
  }
} 