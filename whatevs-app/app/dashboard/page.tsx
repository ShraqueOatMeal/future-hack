'use client'
import { LineChart, Line, ResponsiveContainer } from 'recharts';

const data = [
  { name: 'Jan', value: 400 },
  { name: 'Feb', value: 300 },
  { name: 'Mar', value: 500 },
  { name: 'Apr', value: 600 },
  { name: 'May', value: 550 },
]

export default function Dashboard() {
  return (
    <div className="p-8 space-y-10">
      {/* Cards Section */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
        {/* Monthly Revenue */}
        <div className="card bg-base-100 shadow-xl rounded-xl">
          <div className="card-body p-8">
            <h2 className="card-title text-lg mb-4">Monthly Revenue</h2>
            <p className="text-3xl font-bold text-green-500 mb-6">$8,450</p>
            <div className="h-24 mt-4">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data}>
                  <Line type="monotone" dataKey="value" stroke="#22c55e" strokeWidth={2} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>

        {/* Total Queries */}
        <div className="card bg-base-100 shadow-xl rounded-xl">
          <div className="card-body p-8">
            <h2 className="card-title text-lg mb-4">Total AI Queries</h2>
            <p className="text-3xl font-bold mb-2">1,238</p>
            <p className="text-sm text-gray-500">Since start of this month</p>
          </div>
        </div>

        {/* System Health */}
        <div className="card bg-base-100 shadow-xl rounded-xl">
          <div className="card-body p-8">
            <h2 className="card-title text-lg mb-4">System Health</h2>
            <p className="text-3xl font-bold text-green-500 mb-2">Online</p>
            <p className="text-sm text-gray-500">Last checked: 2 minutes ago</p>
          </div>
        </div>
      </div>

      {/* AI Query & File Upload */}
      <div className="card bg-base-200 rounded-xl shadow-lg">
        <div className="card-body p-8 space-y-6">
          <h2 className="text-xl font-bold mb-2">AI Data Analysis</h2>

          {/* Upload Input */}
          <div className="space-y-2">
            <input type="file" className="file-input file-input-bordered w-full max-w-sm" />
          </div>

          {/* User Query Input */}
          <div className="space-y-2">
            <textarea
              className="textarea textarea-bordered w-full p-4"
              rows={4}
              placeholder="Enter your query (e.g., 'Analyze sales trend by region')"
            ></textarea>
          </div>

          {/* Submit Button */}
          <div className="pt-2">
            <button className="btn btn-primary px-8 py-3">Analyze</button>
          </div>
        </div>
      </div>
    </div>
  )
}