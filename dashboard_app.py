"""
macOS Memory Dashboard - Streamlit application for visualizing system memory metrics.
"""

from datetime import datetime

import pandas as pd
import streamlit as st

from db_readers import (
    fetch_latest_snapshot,
    fetch_system_snapshots,
    fetch_top_processes,
    validate_schema,
)
from openai_advisor import get_latest_recommendations

# Constants
DB_PATH = "system_metrics.db"
DEFAULT_WINDOW_MINUTES = 60
REFRESH_SECONDS = 5


def main():
    """Main dashboard application."""
    st.set_page_config(page_title="macOS Memory Dashboard", layout="wide")
    st.title("macOS Memory Dashboard")
    
    # Validate database schema
    try:
        validate_schema(DB_PATH)
    except RuntimeError as e:
        st.error(f"Database schema validation failed: {e}")
        return
    
    # Sidebar controls
    st.sidebar.header("Settings")
    
    window_options = [15, 30, 60, 180]
    default_index = window_options.index(DEFAULT_WINDOW_MINUTES)
    window_minutes = st.sidebar.selectbox(
        "Time Window (minutes)",
        options=window_options,
        index=default_index
    )
    
    if st.sidebar.button("Refresh now"):
        st.rerun()
    
    # Fetch latest snapshot for headline metrics
    try:
        latest = fetch_latest_snapshot(DB_PATH)
    except RuntimeError as e:
        st.error(f"Error fetching latest snapshot: {e}")
        return
    
    if latest is None:
        st.warning("No data available in database")
        return
    
    # Display headline metrics
    st.subheader("Latest Snapshot")
    
    cols = st.columns(6)
    cols[0].metric("Used (MB)", f"{latest['mem_used_mb']:,}")
    cols[1].metric("Free (MB)", f"{latest['mem_free_mb']:,}")
    cols[2].metric("Compressed (MB)", f"{latest['mem_compressed_mb']:,}")
    cols[3].metric("Swap (MB)", f"{latest['swap_used_mb']:,}")
    cols[4].metric("Pressure", latest['memory_pressure'])
    
    timestamp_dt = datetime.fromtimestamp(latest['timestamp'])
    cols[5].metric("Timestamp", timestamp_dt.strftime("%H:%M:%S"))
    
    st.divider()
    
    # Fetch time series data
    try:
        snapshots = fetch_system_snapshots(DB_PATH, window_minutes)
    except RuntimeError as e:
        st.error(f"Error fetching system snapshots: {e}")
        return
    
    if not snapshots:
        st.warning("No data in selected window")
        return
    
    # Convert to DataFrame with datetime index
    df = pd.DataFrame(snapshots)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df = df.set_index('timestamp')
    
    # Display line charts
    st.subheader(f"Memory Metrics - Last {window_minutes} Minutes")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.line_chart(df[['mem_used_mb']], use_container_width=True)
        st.caption("Memory Used (MB)")
        
        st.line_chart(df[['mem_compressed_mb']], use_container_width=True)
        st.caption("Memory Compressed (MB)")
    
    with col2:
        st.line_chart(df[['mem_free_mb']], use_container_width=True)
        st.caption("Memory Free (MB)")
        
        st.line_chart(df[['swap_used_mb']], use_container_width=True)
        st.caption("Swap Used (MB)")
    
    st.divider()
    
    # Fetch and display top processes
    st.subheader(f"Top Processes - Last {window_minutes} Minutes")
    
    try:
        processes = fetch_top_processes(DB_PATH, window_minutes, limit=25)
    except RuntimeError as e:
        st.error(f"Error fetching top processes: {e}")
        return
    
    if not processes:
        st.info("No process data in selected window")
        return
    
    # Convert to DataFrame and format
    df_processes = pd.DataFrame(processes)
    df_processes['max_rss_mb'] = df_processes['max_rss_mb'].round(1)
    df_processes['avg_rss_mb'] = df_processes['avg_rss_mb'].round(1)
    df_processes['foreground_ratio'] = df_processes['foreground_ratio'].round(2)
    
    st.dataframe(
        df_processes,
        use_container_width=True,
        hide_index=True,
        column_config={
            "process_name": "Process Name",
            "pid": "PID",
            "max_rss_mb": "Max RSS (MB)",
            "avg_rss_mb": "Avg RSS (MB)",
            "times_seen": "Times Seen",
            "foreground_ratio": "Foreground Ratio"
        }
    )
    
    st.divider()
    
    # AI Advisor section
    st.subheader("AI Advisor (Experimental)")
    
    if st.button("Get recommendations"):
        try:
            result = get_latest_recommendations(DB_PATH)
            st.session_state["advisor_result"] = result
        except Exception as e:
            st.error(f"Error getting recommendations: {e}")
    
    if "advisor_result" not in st.session_state:
        st.info("Click to generate recommendations")
    else:
        result = st.session_state["advisor_result"]
        
        # Check for errors
        if "error" in result:
            st.error(f"Error: {result['error']}")
        elif "error" in result.get("recommendations", {}):
            st.error(f"Error: {result['recommendations']['error']}")
        else:
            recs = result.get("recommendations", {})
            
            # Display ranked actions
            ranked_actions = recs.get("ranked_actions", [])
            if ranked_actions:
                st.markdown("**Ranked Actions:**")
                for action in ranked_actions:
                    st.markdown(f"**{action.get('title', 'N/A')}**")
                    st.text(action.get('reason', ''))
                    confidence = action.get('confidence_0_1', 0) * 100
                    st.text(f"Confidence: {confidence:.1f}%")
                    pids = action.get('suggested_pids', [])
                    if pids:
                        st.text(f"Suggested PIDs: {pids}")
                    safety = "safe" if action.get('safe', False) else "unsafe"
                    st.text(f"Safety: {safety}")
                    st.markdown("---")
            
            # Display habit changes
            habit_changes = recs.get("habit_changes", [])
            if habit_changes:
                st.markdown("**Habit Changes:**")
                for habit in habit_changes:
                    title = habit.get('title', 'N/A')
                    reason = habit.get('reason', '')
                    st.markdown(f"- **{title}**: {reason}")
            
            # Display notes
            notes = recs.get("notes", [])
            if notes:
                st.markdown("**Notes:**")
                for note in notes:
                    st.markdown(f"- {note}")


if __name__ == "__main__":
    main()
