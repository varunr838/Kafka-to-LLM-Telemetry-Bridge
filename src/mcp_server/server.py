import os
import json
import pandas as pd
from datetime import datetime, timedelta
from mcp.server.fastmcp import FastMCP
from deltalake import DeltaTable

# Initialize FastMCP server
mcp = FastMCP("Telemetry-Bridge")

# Path to the Gold Delta Table
# We assume the MCP server is run from the workspace root or src/mcp_server
WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
GOLD_PATH = os.environ.get("GOLD_PATH", os.path.join(WORKSPACE_ROOT, 'data', 'gold_logs'))

@mcp.tool()
def get_user_risk_metrics(time_window_minutes: int = 60, user_id: str = None) -> str:
    """
    Analyzes the gold telemetry logs to find user risk metrics (e.g., error rates).
    Reads directly from the structured Delta Lake table.
    
    Args:
        time_window_minutes: How many minutes back to look (default 60).
        user_id: Optional specific user to query. If None, returns top 5 users with most errors.
    """
    if not os.path.exists(GOLD_PATH):
        return json.dumps({"error": f"Delta table not found at {GOLD_PATH}. Has the Spark stream processed any data yet?"})

    try:
        # 1. Load the Delta Table directly (No Spark required, extremely fast)
        dt = DeltaTable(GOLD_PATH)
        
        # 2. Convert to Pandas DataFrame for analysis
        df = dt.to_pandas()
        
        if df.empty:
            return json.dumps({"message": "Delta table is currently empty."})

        # 3. Filter by Time Window
        if 'timestamp' in df.columns:
            # Parse ISO-8601 strings to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Remove timezone for simple comparison (since our generator uses Z)
            df['timestamp'] = df['timestamp'].dt.tz_localize(None) 
            
            cutoff_time = datetime.utcnow() - timedelta(minutes=time_window_minutes)
            df = df[df['timestamp'] >= cutoff_time]
            
        if df.empty:
            return json.dumps({"message": f"No logs found in the last {time_window_minutes} minutes."})

        # 4. Scenario A: Single User Query
        if user_id:
            user_df = df[df['user_id'] == user_id]
            if user_df.empty:
                 return json.dumps({"message": f"No logs found for user {user_id} in the timeframe."})
            
            total_requests = len(user_df)
            errors = user_df[user_df['status_code'] >= 500]
            error_count = len(errors)
            
            result = {
                "user_id": user_id,
                "total_requests": total_requests,
                "error_5xx_count": error_count,
                "error_rate_percentage": round((error_count / total_requests) * 100, 2) if total_requests > 0 else 0
            }
            return json.dumps(result, indent=2)

        # 5. Scenario B: Global Query (Top 5 users with highest errors)
        else:
            # Filter for 500-level errors
            error_df = df[df['status_code'] >= 500]
            
            if error_df.empty:
                return json.dumps({"message": "No 500 errors found in the specified timeframe. Systems are healthy!"})
                
            # Group and count errors
            error_counts = error_df.groupby('user_id').size().reset_index(name='error_count')
            total_counts = df.groupby('user_id').size().reset_index(name='total_requests')
            
            # Merge to calculate rates
            merged = pd.merge(error_counts, total_counts, on='user_id')
            merged['error_rate_percentage'] = round((merged['error_count'] / merged['total_requests']) * 100, 2)
            
            # Sort by error count and take top 5
            top_errors = merged.sort_values(by='error_count', ascending=False).head(5)
            
            # Convert to dictionary for nice JSON formatting
            records = top_errors.to_dict(orient='records')
            return json.dumps({"top_users_with_errors": records}, indent=2)

    except Exception as e:
        return json.dumps({"error": f"Failed to read or process Delta table: {str(e)}"})

if __name__ == "__main__":
    # Start the standard input/output server for Claude Desktop/MCP clients
    mcp.run()
