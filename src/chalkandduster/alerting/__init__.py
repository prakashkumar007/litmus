"""Alerting module - Slack integration and message formatting."""

from chalkandduster.alerting.slack import SlackNotifier, send_slack_alert

__all__ = [
    "SlackNotifier",
    "send_slack_alert",
]

