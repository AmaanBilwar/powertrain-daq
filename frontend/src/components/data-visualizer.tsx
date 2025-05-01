'use client'
import React, { useState, useEffect } from 'react'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'

interface DataVisualizerProps {
  selectedDatabase?: string;
}

const API_BASE_URL = 'http://localhost:5000';

export const DataVisualizer: React.FC<DataVisualizerProps> = ({ selectedDatabase }) => {
  const [columns, setColumns] = useState<string[]>([]);
  const [selectedXAxis, setSelectedXAxis] = useState<string>("");
  const [selectedYAxis, setSelectedYAxis] = useState<string>("");
  const [data, setData] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (selectedDatabase) {
      // Fetch columns for the selected database
      const fetchColumns = async () => {
        try {
          setIsLoading(true);
          setError(null);
          const response = await fetch(`${API_BASE_URL}/api/columns?db=${selectedDatabase}`);
          if (!response.ok) {
            throw new Error('Failed to fetch columns');
          }
          const data = await response.json();
          setColumns(data.columns || []);
        } catch (error) {
          console.error('Error fetching columns:', error);
          setError('Failed to load columns');
          setColumns([]);
        } finally {
          setIsLoading(false);
        }
      };

      fetchColumns();
    }
  }, [selectedDatabase]);

  useEffect(() => {
    if (selectedDatabase && selectedXAxis && selectedYAxis) {
      // Fetch data for the selected axes
      const fetchData = async () => {
        try {
          setIsLoading(true);
          setError(null);
          const response = await fetch(
            `${API_BASE_URL}/api/data?db=${selectedDatabase}&x=${selectedXAxis}&y=${selectedYAxis}`
          );
          if (!response.ok) {
            throw new Error('Failed to fetch data');
          }
          const data = await response.json();
          setData(data);
        } catch (error) {
          console.error('Error fetching data:', error);
          setError('Failed to load data');
          setData([]);
        } finally {
          setIsLoading(false);
        }
      };

      fetchData();
    }
  }, [selectedDatabase, selectedXAxis, selectedYAxis]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Data Visualization</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 gap-4 mb-4">
          <div>
            <label className="block text-sm font-medium mb-2">X-Axis</label>
            <Select value={selectedXAxis} onValueChange={setSelectedXAxis}>
              <SelectTrigger>
                <SelectValue placeholder="Select X-axis" />
              </SelectTrigger>
              <SelectContent>
                {columns.map((column) => (
                  <SelectItem key={column} value={column}>
                    {column}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-2">Y-Axis</label>
            <Select value={selectedYAxis} onValueChange={setSelectedYAxis}>
              <SelectTrigger>
                <SelectValue placeholder="Select Y-axis" />
              </SelectTrigger>
              <SelectContent>
                {columns.map((column) => (
                  <SelectItem key={column} value={column}>
                    {column}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        {isLoading ? (
          <div className="text-sm text-muted-foreground">Loading data...</div>
        ) : error ? (
          <div className="text-sm text-destructive">{error}</div>
        ) : data.length > 0 ? (
          <div className="h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="x" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line
                  type="monotone"
                  dataKey="y"
                  stroke="#8884d8"
                  activeDot={{ r: 8 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="text-sm text-muted-foreground">No data available</div>
        )}
      </CardContent>
    </Card>
  );
}; 