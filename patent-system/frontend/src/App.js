import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { PieChart, Pie, Cell } from 'recharts';
import { TextField, Button, Grid, Paper, Typography, Container, Box, CircularProgress, Alert } from '@mui/material';

// Color palette for visualizations
const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8', '#ffc658'];

// Use a more flexible approach for API URL
// This will try the Docker service name first, then fall back to localhost if needed
const API_URL = process.env.NODE_ENV === 'production' 
  ? 'http://patent-api:5000' 
  : 'http://localhost:5000';

console.log('Using API URL:', API_URL);

function App() {
  // Application state
  const [query, setQuery] = useState('');
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [backendStatus, setBackendStatus] = useState('unknown');

  // Check backend health on component mount
  useEffect(() => {
    const checkBackend = async () => {
      try {
        // First attempt with the service name
        console.log('Checking backend health at:', API_URL);
        const response = await axios.get(`${API_URL}/api/health`, { timeout: 5000 });
        if (response.data.status === 'ok') {
          setBackendStatus('connected');
          console.log('Backend is healthy');
        }
      } catch (err) {
        console.error('Backend health check failed with service name:', err);
        
        // Fall back to localhost as a backup
        try {
          console.log('Trying localhost fallback');
          const response = await axios.get('http://localhost:5000/api/health', { timeout: 5000 });
          if (response.data.status === 'ok') {
            setBackendStatus('connected');
            console.log('Backend is healthy via localhost');
          }
        } catch (fallbackErr) {
          setBackendStatus('disconnected');
          console.error('Backend health check failed completely:', fallbackErr);
        }
      }
    };
    
    checkBackend();
  }, []);

  // Handle search form submission
  const handleSearch = async () => {
    setLoading(true);
    setError(null);
    
    try {
      // Validate query input
      if (!query.trim()) {
        setError('Please enter a search query');
        setLoading(false);
        return;
      }
      
      console.log('Sending query to backend:', query);
      
      // Try the primary API URL
      let response;
      try {
        response = await axios.post(`${API_URL}/api/query`, {
          query: query
        });
      } catch (err) {
        // Fall back to localhost if the primary URL fails
        console.log('Trying localhost fallback for query');
        response = await axios.post('http://localhost:5000/api/query', {
          query: query
        });
      }
      
      console.log('Received response:', response.data);
      
      // Handle different response scenarios
      if (response.data.error) {
        setError(response.data.error);
        console.error('Error from backend:', response.data.error);
      } else if (response.data.patents && response.data.patents.length === 0) {
        setError('No patents found matching your query. Try different search terms.');
      } else {
        setResults(response.data);
      }
    } catch (err) {
      console.error('Error during search:', err);
      setError(`Failed to get results: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  // Rest of the component remains the same...
  // (keeping the same JSX and other functions)

  // Handle Enter key press in search field
  const handleKeyPress = (e) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  return (
    <Container maxWidth="lg">
      <Typography variant="h3" component="h1" align="center" gutterBottom sx={{ mt: 4 }}>
        Patent Visualization System
      </Typography>
      
      {/* Search Form */}
      <Paper elevation={3} sx={{ p: 3, mb: 4 }}>
        <Grid container spacing={2} alignItems="center">
          <Grid item xs={10}>
            <TextField
              fullWidth
              label="Ask about patents (e.g., 'Get me all patents with the word hydrocarbon')"
              variant="outlined"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyPress={handleKeyPress}
              error={!!error}
            />
          </Grid>
          <Grid item xs={2}>
            <Button 
              fullWidth
              variant="contained" 
              color="primary" 
              onClick={handleSearch}
              disabled={loading || backendStatus === 'disconnected'}
            >
              {loading ? <CircularProgress size={24} color="inherit" /> : 'Search'}
            </Button>
          </Grid>
          {/* Display connection error if backend is unreachable */}
          {backendStatus === 'disconnected' && (
            <Grid item xs={12}>
              <Alert severity="error">
                Cannot connect to backend service. Please ensure the backend is running.
              </Alert>
            </Grid>
          )}
          {/* Display any search or processing errors */}
          {error && (
            <Grid item xs={12}>
              <Alert severity="warning">{error}</Alert>
            </Grid>
          )}
        </Grid>
      </Paper>

      {/* Results Section - Only shown when we have results */}
      {results && (
        <Grid container spacing={4}>
          {/* Results Header */}
          <Grid item xs={12}>
            <Paper elevation={2} sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Found {results.total_count} patents
              </Typography>
            </Paper>
          </Grid>
          
          {/* Timeline Chart - Shows patents over time */}
          <Grid item xs={12} md={6}>
            <Paper elevation={2} sx={{ p: 2, height: 300 }}>
              <Typography variant="h6" gutterBottom>
                Patents Over Time
              </Typography>
              {results.timeline && results.timeline.length > 0 ? (
                <ResponsiveContainer width="100%" height="80%">
                  <LineChart data={results.timeline}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="year" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Line type="monotone" dataKey="count" stroke="#8884d8" activeDot={{ r: 8 }} />
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <Box display="flex" justifyContent="center" alignItems="center" height="80%">
                  <Typography variant="body1">No timeline data available</Typography>
                </Box>
              )}
            </Paper>
          </Grid>
          
          {/* CPC Sections Pie Chart - Shows distribution by category */}
          <Grid item xs={12} md={6}>
            <Paper elevation={2} sx={{ p: 2, height: 300 }}>
              <Typography variant="h6" gutterBottom>
                CPC Sections Distribution
              </Typography>
              {results.cpc_sections && results.cpc_sections.length > 0 ? (
                <ResponsiveContainer width="100%" height="80%">
                  <PieChart>
                    <Pie
                      data={results.cpc_sections}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      outerRadius={80}
                      fill="#8884d8"
                      dataKey="count"
                      nameKey="section"
                      label={({ section, percent }) => `${section}: ${(percent * 100).toFixed(0)}%`}
                    >
                      {results.cpc_sections.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip />
                  </PieChart>
                </ResponsiveContainer>
              ) : (
                <Box display="flex" justifyContent="center" alignItems="center" height="80%">
                  <Typography variant="body1">No CPC section data available</Typography>
                </Box>
              )}
            </Paper>
          </Grid>
          
          {/* Top Inventors Section - Shows bar chart of top contributors */}
          <Grid item xs={12}>
            <Paper elevation={2} sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Top Inventors
              </Typography>
              <div style={{ height: 300, overflowY: 'auto' }}>
                {results.inventors && results.inventors.length > 0 ? (
                  results.inventors.map((inventor, index) => (
                    <div key={index} style={{ display: 'flex', margin: '8px 0' }}>
                      <div style={{ width: '60%' }}>{inventor.name}</div>
                      <div style={{ width: '40%' }}>
                        <div style={{ 
                          background: COLORS[index % COLORS.length], 
                          height: 20, 
                          width: `${Math.min(100, inventor.count/results.inventors[0].count*100)}%` 
                        }}></div>
                      </div>
                      <div style={{ marginLeft: 8 }}>{inventor.count}</div>
                    </div>
                  ))
                ) : (
                  <Typography variant="body1">No inventor data available</Typography>
                )}
              </div>
            </Paper>
          </Grid>
          
          {/* Patent List - Shows details of individual patents */}
          <Grid item xs={12}>
            <Paper elevation={2} sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Patent List
              </Typography>
              <div style={{ height: 400, overflowY: 'auto' }}>
                {results.patents && results.patents.length > 0 ? (
                  results.patents.map((patent, index) => (
                    <Paper 
                      key={index} 
                      elevation={1} 
                      sx={{ p: 2, mb: 2, backgroundColor: '#f9f9f9' }}
                    >
                      <Typography variant="subtitle1" fontWeight="bold">
                        {patent.title || 'Untitled Patent'}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        ID: {patent.id} • Date: {patent.date || 'Unknown'} 
                        {patent.num_claims ? ` • Claims: ${patent.num_claims}` : ''}
                      </Typography>
                      <Typography variant="body2" sx={{ mt: 1 }}>
                        {patent.abstract ? (
                          patent.abstract.length > 200 ? 
                          patent.abstract.substring(0, 200) + '...' : 
                          patent.abstract
                        ) : 'No abstract available'}
                      </Typography>
                      {patent.cpc_classes && patent.cpc_classes.length > 0 && (
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px', marginTop: '8px' }}>
                          {patent.cpc_classes.slice(0, 5).map((cpc, i) => (
                            <span 
                              key={i} 
                              style={{ 
                                backgroundColor: COLORS[i % COLORS.length], 
                                color: 'white', 
                                padding: '2px 6px', 
                                borderRadius: '4px',
                                fontSize: '0.75rem'
                              }}
                            >
                              {cpc}
                            </span>
                          ))}
                          {patent.cpc_classes.length > 5 && (
                            <span style={{ fontSize: '0.75rem' }}>+{patent.cpc_classes.length - 5} more</span>
                          )}
                        </div>
                      )}
                      {patent.inventors && patent.inventors.length > 0 && (
                        <Typography variant="body2" sx={{ mt: 1, fontStyle: 'italic' }}>
                          Inventors: {patent.inventors.map(inv => inv.name).join(', ')}
                        </Typography>
                      )}
                    </Paper>
                  ))
                ) : (
                  <Typography variant="body1">No patent data available</Typography>
                )}
              </div>
            </Paper>
          </Grid>
        </Grid>
      )}
    </Container>
  );
}

export default App;
