# Contributing to Sustainable Building Energy Benchmarking Pipeline

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing to the project.

## Quick Start for Contributors

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/Sustainable-Building-Energy-Benchmarking-Pipeline.git
   cd Sustainable-Building-Energy-Benchmarking-Pipeline
   ```

2. **Set up your development environment**
   ```bash
   make install
   ```

3. **Generate sample data for testing**
   ```bash
   make sample-data
   ```

4. **Run tests to verify setup**
   ```bash
   make test
   ```

## Development Workflow

### Making Changes

1. **Create a new branch** for your feature or bug fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following the coding standards below

3. **Test your changes**:
   ```bash
   make test
   ```

4. **Commit your changes** with clear, descriptive messages:
   ```bash
   git commit -m "Add feature: description of what was added"
   ```

5. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request** on GitHub

### Testing

All changes should include appropriate tests:

- **Unit tests** for new functions or classes
- **Integration tests** for API endpoints or pipeline components
- Place tests in the `tests/` directory following existing patterns

Run tests before submitting:
```bash
# Run all tests
make test

# Run with coverage report
make test-cov

# Run specific test file
pytest tests/test_your_module.py -v
```

All existing tests must pass before a PR can be merged.

## Coding Standards

### Python Style

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guide
- Use meaningful variable and function names
- Add docstrings to all functions and classes
- Keep functions focused and small (single responsibility)

### Documentation

- Update README.md if adding new features or changing usage
- Add docstrings following Google style format:
  ```python
  def example_function(param1, param2):
      """
      Brief description of function.
      
      Args:
          param1: Description of param1
          param2: Description of param2
          
      Returns:
          Description of return value
      """
  ```

### Commit Messages

Write clear commit messages:
- Use present tense ("Add feature" not "Added feature")
- Keep first line under 50 characters
- Add detailed explanation in body if needed
- Reference issue numbers when applicable

Example:
```
Add anomaly detection for HVAC systems

Implement Isolation Forest model for detecting anomalies
in HVAC sensor data. Includes training pipeline and 
prediction endpoint.

Fixes #123
```

## Project Structure

Understanding the project layout:

```
├── api/                # Benchmarking REST API
├── benchmarking/       # Core benchmarking logic
├── src/               # HVAC fault detection system
├── tests/             # Test suite
├── data/              # Generated data (git-ignored)
├── sample_data/       # Sample benchmarking data (git-ignored)
├── notebooks/         # Jupyter notebooks
└── requirements.txt   # Python dependencies
```

## Adding New Features

### Adding a New Benchmarking Metric

1. Add the calculation logic to `benchmarking/model.py`
2. Update API models in `api/main.py` if needed
3. Add tests in `tests/test_benchmarking.py`
4. Update documentation

### Adding a New HVAC Fault Type

1. Add fault injection logic to `src/generate_hvac_data.py`
2. Add detection rule to `src/models.py`
3. Add tests in `tests/test_models.py`
4. Update HVAC_README.md with fault description

### Adding a New API Endpoint

1. Add endpoint to `api/main.py` or `src/api.py`
2. Add request/response models using Pydantic
3. Add integration tests
4. Update API documentation in README

## Common Tasks

### Regenerate Sample Data
```bash
make clean
make sample-data
```

### Update Dependencies
```bash
# Add new dependency to requirements.txt
pip install -r requirements.txt
make test  # Verify nothing broke
```

### Run Local Services
```bash
# Start PostgreSQL database
make docker-up

# Start API server (in one terminal)
make run-api

# Start dashboard (in another terminal)
make run-dashboard
```

### Clean Generated Files
```bash
make clean
```

## Reporting Issues

When reporting bugs, please include:

1. **Description** of the issue
2. **Steps to reproduce** the problem
3. **Expected behavior** vs actual behavior
4. **Environment details** (OS, Python version, etc.)
5. **Error messages** or logs if applicable

## Getting Help

- Check existing [Issues](https://github.com/shahabsalehi/Sustainable-Building-Energy-Benchmarking-Pipeline/issues)
- Review the [README.md](README.md) and [HVAC_README.md](HVAC_README.md)
- Ask questions by opening a new issue with the "question" label

## Code Review Process

All submissions require review:

1. PRs must pass all automated tests
2. Code must follow project standards
3. Documentation must be updated
4. At least one maintainer approval required

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.


Thank you for contributing to making building energy management more sustainable!
