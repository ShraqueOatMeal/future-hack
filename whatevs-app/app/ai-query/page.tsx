"use client";
import { useState } from "react";

export default function AIQueryPage() {
  const [query, setQuery] = useState("");
  const [response, setResponse] = useState("");

  const handleSubmit = () => {
    setResponse(`Pretend this is the AI responding to: "${query}"`);
    setQuery("");
  };

  return (
    <div className="flex flex-col items-center w-full">
      <div className="card w-full md:w-3/4 lg:w-2/3 bg-base-100 shadow-xl mt-10">
        <div className="card-body">
          <h2 className="card-title">AI Query Assistant</h2>

          {/* AI Response Display */}
          <div className="mockup-code bg-base-200 text-base-content min-h-[100px]">
            {response ? (
              <pre data-prefix="AI">
                <code>{response}</code>
              </pre>
            ) : (
              <pre data-prefix="AI" className="text-gray-400">
                <code>The AI&apos;s response will appear here.</code>
              </pre>
            )}
          </div>

          {/* Input & Button */}
          <textarea
            className="textarea textarea-bordered mt-4 w-full"
            placeholder="Kowalski analysis!"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          ></textarea>

          <div className="card-actions justify-end mt-2">
            <button className="btn btn-primary" onClick={handleSubmit}>
              Ask AI
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

