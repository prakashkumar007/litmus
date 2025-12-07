"""
Chalk and Duster - Slack Alerting
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
import structlog

from chalkandduster.core.config import settings
from chalkandduster.core.exceptions import ChalkAndDusterError

logger = structlog.get_logger()


class SlackNotifier:
    """
    Slack notification sender.
    
    Supports both simple messages and rich Block Kit messages.
    """
    
    def __init__(
        self,
        webhook_url: Optional[str] = None,
        default_channel: Optional[str] = None,
    ):
        self.webhook_url = webhook_url or settings.SLACK_WEBHOOK_URL
        self.default_channel = default_channel or settings.SLACK_DEFAULT_CHANNEL
    
    async def send_message(
        self,
        text: str,
        channel: Optional[str] = None,
        blocks: Optional[List[Dict[str, Any]]] = None,
    ) -> bool:
        """
        Send a message to Slack.
        
        Args:
            text: The message text (fallback for notifications)
            channel: Optional channel override
            blocks: Optional Block Kit blocks for rich formatting
            
        Returns:
            True if message was sent successfully
        """
        if not self.webhook_url:
            logger.warning("Slack webhook URL not configured")
            return False
        
        payload: Dict[str, Any] = {"text": text}
        
        if channel:
            payload["channel"] = channel
        
        if blocks:
            payload["blocks"] = blocks
        
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(
                    self.webhook_url,
                    json=payload,
                )
                
                if response.status_code == 200:
                    logger.info("Slack message sent", channel=channel or self.default_channel)
                    return True
                else:
                    logger.error(
                        "Slack API error",
                        status_code=response.status_code,
                        response=response.text,
                    )
                    return False
                    
        except Exception as e:
            logger.error("Failed to send Slack message", error=str(e))
            return False
    
    async def send_quality_alert(
        self,
        dataset_name: str,
        run_id: str,
        failures: List[Dict[str, Any]],
        summary: str,
        severity: str = "warning",
        channel: Optional[str] = None,
    ) -> bool:
        """
        Send a data quality alert to Slack.
        
        Args:
            dataset_name: Name of the dataset
            run_id: The run ID
            failures: List of failed checks
            summary: LLM-generated summary
            severity: Alert severity (critical, warning, info)
            channel: Optional channel override
            
        Returns:
            True if alert was sent successfully
        """
        blocks = self._build_quality_alert_blocks(
            dataset_name=dataset_name,
            run_id=run_id,
            failures=failures,
            summary=summary,
            severity=severity,
        )
        
        text = f"Data Quality Alert: {dataset_name} - {len(failures)} check(s) failed"
        
        return await self.send_message(text=text, channel=channel, blocks=blocks)
    
    async def send_drift_alert(
        self,
        dataset_name: str,
        run_id: str,
        drift_results: List[Dict[str, Any]],
        summary: str,
        channel: Optional[str] = None,
    ) -> bool:
        """
        Send a drift detection alert to Slack.
        
        Args:
            dataset_name: Name of the dataset
            run_id: The run ID
            drift_results: List of drift detection results
            summary: LLM-generated summary
            channel: Optional channel override
            
        Returns:
            True if alert was sent successfully
        """
        blocks = self._build_drift_alert_blocks(
            dataset_name=dataset_name,
            run_id=run_id,
            drift_results=drift_results,
            summary=summary,
        )
        
        drift_count = sum(1 for r in drift_results if r.get("detected"))
        text = f"Drift Alert: {dataset_name} - {drift_count} drift(s) detected"
        
        return await self.send_message(text=text, channel=channel, blocks=blocks)
    
    def _build_quality_alert_blocks(
        self,
        dataset_name: str,
        run_id: str,
        failures: List[Dict[str, Any]],
        summary: str,
        severity: str,
    ) -> List[Dict[str, Any]]:
        """Build Block Kit blocks for quality alert."""
        emoji = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}.get(severity, "⚠️")
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} Data Quality Alert: {dataset_name}",
                },
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Summary:* {summary}"},
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Failed Checks ({len(failures)}):*",
                },
            },
        ]
        
        # Add failure details (limit to 5)
        for failure in failures[:5]:
            check_name = failure.get("check_name", "Unknown")
            message = failure.get("message", "No details")
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"• `{check_name}`: {message}"},
            })
        
        if len(failures) > 5:
            blocks.append({
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"_...and {len(failures) - 5} more_"},
                ],
            })
        
        # Add footer
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"Run ID: `{run_id}` | {datetime.utcnow().isoformat()}"},
            ],
        })
        
        return blocks
    
    def _build_drift_alert_blocks(
        self,
        dataset_name: str,
        run_id: str,
        drift_results: List[Dict[str, Any]],
        summary: str,
    ) -> List[Dict[str, Any]]:
        """Build Block Kit blocks for drift alert."""
        detected = [r for r in drift_results if r.get("detected")]
        
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"📊 Drift Alert: {dataset_name}"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Summary:* {summary}"},
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Drift Detected ({len(detected)}):*"},
            },
        ]
        
        for result in detected[:5]:
            monitor_name = result.get("monitor_name", "Unknown")
            drift_type = result.get("drift_type", "unknown")
            message = result.get("message", "No details")
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"• *{monitor_name}* ({drift_type}): {message}"},
            })
        
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"Run ID: `{run_id}` | {datetime.utcnow().isoformat()}"},
            ],
        })
        
        return blocks


async def send_slack_alert(
    message: str,
    webhook_url: Optional[str] = None,
    channel: Optional[str] = None,
) -> bool:
    """
    Convenience function to send a simple Slack alert.
    
    Args:
        message: The message to send
        webhook_url: Optional webhook URL override
        channel: Optional channel override
        
    Returns:
        True if message was sent successfully
    """
    notifier = SlackNotifier(webhook_url=webhook_url)
    return await notifier.send_message(text=message, channel=channel)

