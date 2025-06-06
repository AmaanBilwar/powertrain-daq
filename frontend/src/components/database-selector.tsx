'use client'
import React, { useState, useEffect } from 'react'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

interface DatabaseSelectorProps {
  onDatabaseSelect?: (dbName: string) => void;
}

const API_BASE_URL = 'http://localhost:5000';

export const DatabaseSelector: React.FC<DatabaseSelectorProps> = ({ onDatabaseSelect }) => {
  const [databases, setDatabases] = useState<string[]>([]);
  const [selectedDatabase, setSelectedDatabase] = useState<string>("");
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Fetch available databases from the backend
    const fetchDatabases = async () => {
      try {
        setIsLoading(true);
        setError(null);
        console.log('Fetching databases from:', `${API_BASE_URL}/api/databases`);
        const response = await fetch(`${API_BASE_URL}/api/databases`);
        console.log('Response status:', response.status);
        if (!response.ok) {
          throw new Error(`Failed to fetch databases: ${response.status}`);
        }
        const data = await response.json();
        console.log('Received data:', data);
        setDatabases(data.databases || []);
      } catch (error) {
        console.error('Error fetching databases:', error);
        setError('Failed to load databases');
        setDatabases([]);
      } finally {
        setIsLoading(false);
      }
    };

    fetchDatabases();
  }, []);

  const handleDatabaseSelect = (value: string) => {
    console.log('Selected database:', value);
    setSelectedDatabase(value);
    if (onDatabaseSelect) {
      onDatabaseSelect(value);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Select Database</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="text-sm text-muted-foreground">Loading databases...</div>
        ) : error ? (
          <div className="text-sm text-destructive">{error}</div>
        ) : databases.length === 0 ? (
          <div className="text-sm text-muted-foreground">No databases available</div>
        ) : (
          <Select value={selectedDatabase} onValueChange={handleDatabaseSelect}>
            <SelectTrigger className="w-[300px]">
              <SelectValue placeholder="Select a database" />
            </SelectTrigger>
            <SelectContent>
              {databases.map((db) => (
                <SelectItem key={db} value={db}>
                  {db}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        )}
      </CardContent>
    </Card>
  );
}; 