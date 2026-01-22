"""
OpenAI-powered recommendation engine for memory optimization.
"""

import json
import os
import sys
from typing import Any

from openai import OpenAI

from advisor_summary import build_advisor_summary


DB_PATH = "system_metrics.db"
MODEL = "gpt-4.1-mini"


def build_recommendations(summary: dict) -> dict[str, Any]:
    """
    Convert advisor summary into actionable recommendations using OpenAI.
    
    Args:
        summary: Dictionary from build_advisor_summary
        
    Returns:
        Dictionary with recommendations or error information
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return {"error": "missing_api_key"}
    
    client = OpenAI(api_key=api_key)
    
    system_prompt = """You are a macOS memory optimization advisor. Analyze the provided memory statistics and return recommendations as STRICT JSON only.

Required JSON structure:
{
  "ranked_actions": [
    {
      "title": "action description",
      "reason": "why this helps",
      "confidence_0_1": 0.85,
      "suggested_pids": [1234, 5678],
      "safe": true
    }
  ],
  "habit_changes": [
    {
      "title": "habit change",
      "reason": "long-term benefit"
    }
  ],
  "notes": ["general observation"]
}

Prioritize safe actions (close/quit) over force-kill unless confidence is very high. Return only valid JSON."""

    user_prompt = f"Memory statistics:\n{json.dumps(summary, indent=2)}\n\nProvide recommendations as JSON."
    
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=2000
        )
        
        raw_text = response.choices[0].message.content.strip()
        
        try:
            recommendations = json.loads(raw_text)
            return recommendations
        except json.JSONDecodeError:
            return {"error": "json_parse_failed", "raw": raw_text}
            
    except Exception as e:
        return {"error": "api_call_failed", "reason": str(e)}


def get_latest_recommendations(db_path: str) -> dict[str, Any]:
    """
    Get advisor summary and build recommendations.
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        Dictionary with summary and recommendations
    """
    summary = build_advisor_summary(db_path)
    recommendations = build_recommendations(summary)
    
    return {
        "summary": summary,
        "recommendations": recommendations
    }


def main():
    """CLI entry point."""
    try:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            print(json.dumps({"error": "missing_api_key"}, ensure_ascii=True))
            sys.exit(1)
        
        result = get_latest_recommendations(DB_PATH)
        print(json.dumps(result, ensure_ascii=True))
        sys.exit(0)
        
    except Exception as e:
        print(json.dumps({"error": "exception", "reason": str(e)}, ensure_ascii=True))
        sys.exit(1)


if __name__ == "__main__":
    main()
