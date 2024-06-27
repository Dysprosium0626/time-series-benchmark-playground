use std::{
    io::Write,
    process::{Command, Stdio},
};

pub struct Usql {
    command: Command,
}

impl Usql {
    pub fn new(command_args: &str) -> Self {
        let mut command = Command::new("usql");
        command.arg(command_args);
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());

        Usql { command }
    }

    pub fn execute(&mut self, sql: &str) -> Result<String, String> {
        let mut child = self.command.spawn().map_err(|e| e.to_string())?;

        {
            let stdin = child.stdin.as_mut().ok_or("Failed to open stdin")?;
            stdin.write_all(sql.as_bytes()).map_err(|e| e.to_string())?;
        }

        let output = child.wait_with_output().map_err(|e| e.to_string())?;

        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).to_string());
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}
