"""
Job execution module - runs commands and captures results.
"""

import subprocess
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    """Result of command execution."""
    returncode: int
    stdout: str
    stderr: str
    duration: float


class Executor:
    """Executes job commands via subprocess."""
    
    def __init__(self, timeout: int = 3600):
        """
        Initialize executor.
        
        Args:
            timeout: Maximum execution time in seconds
        """
        self.timeout = timeout
    
    def execute(self, command: str) -> ExecutionResult:
        """
        Execute a command and return the result.
        
        Args:
            command: Shell command to execute
            
        Returns:
            ExecutionResult with returncode, stdout, stderr
        
        Note:
            Uses shell=True for command flexibility.
            SECURITY WARNING: Only use with trusted commands!
        """
        import time
        
        start_time = time.time()
        
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=self.timeout
            )
            
            duration = time.time() - start_time
            
            return ExecutionResult(
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                duration=duration
            )
        
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            logger.error(f"Command timed out after {self.timeout}s: {command}")
            return ExecutionResult(
                returncode=-1,
                stdout="",
                stderr=f"Command timed out after {self.timeout} seconds",
                duration=duration
            )
        
        except FileNotFoundError as e:
            duration = time.time() - start_time
            logger.error(f"Command not found: {command}")
            return ExecutionResult(
                returncode=127,
                stdout="",
                stderr=f"Command not found: {e}",
                duration=duration
            )
        
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Execution error: {e}")
            return ExecutionResult(
                returncode=-1,
                stdout="",
                stderr=f"Execution error: {str(e)}",
                duration=duration
            )