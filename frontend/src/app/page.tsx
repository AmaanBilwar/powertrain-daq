'use client'

import React from 'react'
import { DatabaseSelector } from '@/components/database-selector'
import { DataVisualizer } from '@/components/data-visualizer'

const Page = () => {
  const [selectedDatabase, setSelectedDatabase] = React.useState<string>('')

  return (
    <main className="container mx-auto p-4">
      <div className="mb-8">
        <h1 className="text-3xl font-bold mb-4">CAN Message Viewer</h1>
        <DatabaseSelector onDatabaseSelect={setSelectedDatabase} />
      </div>
      <DataVisualizer selectedDatabase={selectedDatabase} />
    </main>
  )
}

export default Page