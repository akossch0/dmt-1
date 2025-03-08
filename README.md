# Data Management for Transportation - Design and modelling for transportation data

Barcelona Bicing service - Urban infrastructure planning

## Project Structure

```
├── data/               # Data files (raw, processed, etc.)
├── src/                # Source code for project
│   ├── data/           # Scripts for data processing
│   └── visualization/  # Scripts for data visualization
├── notebooks/          # Jupyter notebooks
├── requirements.txt    # Project dependencies
└── README.md           # Project documentation
```

## Setup Instructions

### Prerequisites
- Python 3.9 or higher
- pip (Python package installer)

### Installation

1. Clone this repository
   ```bash
   git clone [repository URL]
   cd [repository name]
   ```

2. Create and activate a virtual environment (recommended)
   ```bash
   # Using venv
   python -m venv .venv
   
   # On Windows
   .venv\Scripts\activate
   
   # On macOS/Linux
   source .venv/bin/activate
   ```

3. Install the required packages
   ```bash
   pip install -r requirements.txt
   ```

## Usage

- Put your data files in the `data/` directory
- Develop your code in the `src/` directory
- Use `notebooks/` for exploratory data analysis
