// Example using React and Axios

import React, { useEffect, useState } from 'react';
import axios from 'axios';

function App() {
  const [options, setOptions] = useState([]);

  useEffect(() => {
    axios.get('http://localhost:8000/options/?limit=50')
      .then(response => {
        setOptions(response.data);
      })
      .catch(error => {
        console.error('Error fetching option prices:', error);
      });
  }, []);

  return (
    <div>
      <h1>Real-Time Bitcoin Option Prices</h1>
      <table>
        <thead>
          <tr>
            <th>Option ID</th>
            <th>Timestamp</th>
            <th>Underlying Price</th>
            <th>Strike Price</th>
            <th>Volatility</th>
            <th>Interest Rate</th>
            <th>Call Price</th>
            <th>Put Price</th>
            <th>Pricing Method</th>
          </tr>
        </thead>
        <tbody>
          {options.map(option => (
            <tr key={option.option_id}>
              <td>{option.option_id}</td>
              <td>{option.timestamp}</td>
              <td>{option.underlying_price}</td>
              <td>{option.strike_price}</td>
              <td>{option.volatility}</td>
              <td>{option.interest_rate}</td>
              <td>{option.option_price_call}</td>
              <td>{option.option_price_put}</td>
              <td>{option.pricing_method}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;
