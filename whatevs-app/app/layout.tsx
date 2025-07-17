import './globals.css'
import Link from 'next/link'

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" data-theme="autumn">
      <body>
        <div className="drawer lg:drawer-open">
          <input id="sidebar" type="checkbox" className="drawer-toggle" />
          <div className="drawer-content">
            {children}
          </div>
          <div className="drawer-side">
            <label htmlFor="sidebar" className="drawer-overlay"></label>
            <ul className="menu p-4 w-40 h-50 bg-base-200 text-base-content rounded-lg ml-4">
              <li>
                <Link href="/dashboard" className="pl-4">Dashboard</Link>
              </li>
              <li>
                <Link href="/ai-query" className="pl-4">AI Query</Link>
              </li>
            </ul>
          </div>
        </div>
      </body>
    </html>
  )
}