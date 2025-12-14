#!/usr/bin/env python3
"""
Chalk and Duster - Local Lambda Testing Script

This script allows you to test Lambda functions locally without deploying to AWS.
Supports both direct invocation and LocalStack Lambda emulation.

Usage:
    # Direct invocation (fastest, no LocalStack needed)
    python scripts/lambda_local.py invoke quality --event tests/events/quality_event.json
    python scripts/lambda_local.py invoke drift --event tests/events/drift_event.json
    python scripts/lambda_local.py invoke baseline --event tests/events/baseline_event.json

    # Deploy to LocalStack and invoke
    python scripts/lambda_local.py deploy --function quality
    python scripts/lambda_local.py deploy --function drift
    python scripts/lambda_local.py deploy --function baseline

    # Invoke deployed Lambda in LocalStack
    python scripts/lambda_local.py invoke-remote quality --event tests/events/quality_event.json
"""

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path

# Add src to path for local imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Add lambda folder to path (can't use 'lambda' as module name - it's a Python keyword)
LAMBDA_DIR = Path(__file__).parent.parent / "lambda"


def load_handler(function_name: str):
    """Dynamically load a Lambda handler from the lambda folder."""
    handler_file = LAMBDA_DIR / f"{function_name}_handler.py"

    if not handler_file.exists():
        raise ValueError(f"Handler not found: {handler_file}")

    # Load the module dynamically
    spec = importlib.util.spec_from_file_location(f"{function_name}_handler", handler_file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[f"{function_name}_handler"] = module
    spec.loader.exec_module(module)

    return module.handler


def direct_invoke(function_name: str, event: dict) -> dict:
    """Directly invoke the Lambda handler function (fastest for testing)."""

    if function_name not in ["quality", "drift", "baseline"]:
        raise ValueError(f"Unknown function: {function_name}")

    handler = load_handler(function_name)

    # Create a mock Lambda context
    class MockContext:
        memory_limit_in_mb = 256
        aws_request_id = "test-request-id"

        def __init__(self, fn_name: str):
            self.function_name = fn_name
            self.invoked_function_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fn_name}"

        def get_remaining_time_in_millis(self):
            return 300000  # 5 minutes

    return handler(event, MockContext(function_name))


def deploy_to_localstack(function_name: str) -> None:
    """Deploy Lambda function to LocalStack."""
    import subprocess
    import zipfile
    import tempfile

    # Map function names to handler modules (using lambda_handlers as module name in zip)
    handler_map = {
        "quality": "lambda_handlers.quality_handler.handler",
        "drift": "lambda_handlers.drift_handler.handler",
        "baseline": "lambda_handlers.baseline_handler.handler",
    }

    if function_name not in handler_map:
        raise ValueError(f"Unknown function: {function_name}")

    # Create deployment package
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / f"{function_name}.zip"

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Add lambda handlers (using lambda_handlers as package name to avoid Python keyword)
            lambda_dir = Path(__file__).parent.parent / "lambda"
            for py_file in lambda_dir.glob("*.py"):
                zf.write(py_file, f"lambda_handlers/{py_file.name}")
            # Add __init__.py for the package
            zf.writestr("lambda_handlers/__init__.py", "")
            
            # Add src modules
            src_dir = Path(__file__).parent.parent / "src" / "chalkandduster"
            for py_file in src_dir.rglob("*.py"):
                rel_path = py_file.relative_to(src_dir.parent)
                zf.write(py_file, str(rel_path))
        
        # Deploy to LocalStack
        endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
        
        # Create function
        cmd = [
            "aws", "lambda", "create-function",
            "--function-name", f"chalkandduster-{function_name}",
            "--runtime", "python3.11",
            "--handler", handler_map[function_name],
            "--zip-file", f"fileb://{zip_path}",
            "--role", "arn:aws:iam::000000000000:role/lambda-role",
            "--endpoint-url", endpoint_url,
            "--timeout", "300",
            "--memory-size", "512",
        ]
        
        try:
            subprocess.run(cmd, check=True, capture_output=True)
            print(f"✅ Deployed {function_name} to LocalStack")
        except subprocess.CalledProcessError:
            # Try update if create fails
            cmd[2] = "update-function-code"
            cmd = cmd[:3] + ["--function-name", f"chalkandduster-{function_name}", 
                            "--zip-file", f"fileb://{zip_path}",
                            "--endpoint-url", endpoint_url]
            subprocess.run(cmd, check=True)
            print(f"✅ Updated {function_name} in LocalStack")


def invoke_remote(function_name: str, event: dict) -> dict:
    """Invoke Lambda function deployed in LocalStack."""
    import subprocess
    import tempfile
    
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(event, f)
        payload_file = f.name
    
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
        output_file = f.name
    
    cmd = [
        "aws", "lambda", "invoke",
        "--function-name", f"chalkandduster-{function_name}",
        "--payload", f"fileb://{payload_file}",
        "--endpoint-url", endpoint_url,
        output_file,
    ]
    
    subprocess.run(cmd, check=True)
    
    with open(output_file) as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="Local Lambda Testing")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Direct invoke
    invoke_parser = subparsers.add_parser("invoke", help="Directly invoke handler")
    invoke_parser.add_argument("function", choices=["quality", "drift", "baseline"])
    invoke_parser.add_argument("--event", "-e", required=True, help="Event JSON file")
    
    # Deploy
    deploy_parser = subparsers.add_parser("deploy", help="Deploy to LocalStack")
    deploy_parser.add_argument("--function", "-f", required=True, 
                               choices=["quality", "drift", "baseline", "all"])
    
    # Remote invoke
    remote_parser = subparsers.add_parser("invoke-remote", help="Invoke in LocalStack")
    remote_parser.add_argument("function", choices=["quality", "drift", "baseline"])
    remote_parser.add_argument("--event", "-e", required=True, help="Event JSON file")
    
    args = parser.parse_args()
    
    if args.command == "invoke":
        with open(args.event) as f:
            event = json.load(f)
        result = direct_invoke(args.function, event)
        print(json.dumps(result, indent=2, default=str))
    
    elif args.command == "deploy":
        functions = ["quality", "drift", "baseline"] if args.function == "all" else [args.function]
        for fn in functions:
            deploy_to_localstack(fn)
    
    elif args.command == "invoke-remote":
        with open(args.event) as f:
            event = json.load(f)
        result = invoke_remote(args.function, event)
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

