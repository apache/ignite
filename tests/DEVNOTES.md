# Development Notes: Running BATS Tests

This project uses the Bash Automated Testing System (BATS) for testing shell scripts.

## 1. Prerequisites Installation

You must install the BATS core framework on your local machine.

### macOS (via Homebrew)
```bash
brew install bats-core
```

### Linux (Ubuntu/Debian)
```bash
sudo apt-get update
sudo apt-get install bats
```

## 2. Running Tests in IntelliJ IDEA

Since we are using the free built-in **Shell Script** plugin, tests are executed via the integrated terminal.

1. Open the built-in terminal in IntelliJ IDEA (`Alt + F12` or `Option + F12`).
2. Run all tests in the directory:
   ```bash
   bats .
   ```
3. Run a specific test file:
   ```bash
   bats test_script.bats
   ```
