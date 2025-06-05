#!/usr/bin/env python3
"""
Smart HVAC Monitoring System - Environment Setup Script
Automated setup for the complete system
"""
import os
import sys
import subprocess
import platform
from pathlib import Path

class SmartHVACSetup:
    """Setup manager for Smart HVAC system"""
    
    def __init__(self):
        self.system_os = platform.system()
        self.project_root = Path(__file__).parent.parent
        self.python_executable = sys.executable
        
        print("""
╔══════════════════════════════════════════════════════════════════╗
║            Smart HVAC Monitoring System Setup                   ║
║                      Version 1.0.0                              ║
╚══════════════════════════════════════════════════════════════════╝
        """)
    
    def check_python_version(self):
        """Check Python version compatibility"""
        print("🐍 Checking Python version...")
        
        version = sys.version_info
        if version.major < 3 or (version.major == 3 and version.minor < 8):
            print(f"❌ Python 3.8+ required. Found: {version.major}.{version.minor}")
            return False
        
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} detected")
        return True
    
    def create_project_structure(self):
        """Create necessary project directories"""
        print("📁 Creating project structure...")
        
        directories = [
            'sensors',
            'edge_processing',
            'web_dashboard/templates',
            'web_dashboard/static/css',
            'web_dashboard/static/js',
            'web_dashboard/api',
            'database',
            'config',
            'utils',
            'tests',
            'scripts',
            'logs',
            'cloud_simulation'
        ]
        
        for directory in directories:
            dir_path = self.project_root / directory
            dir_path.mkdir(parents=True, exist_ok=True)
            
            # Create __init__.py for Python packages
            if not directory.startswith(('logs', 'web_dashboard/static', 'web_dashboard/templates')):
                init_file = dir_path / '__init__.py'
                if not init_file.exists():
                    init_file.touch()
            
            print(f"  ✅ {directory}")
        
        print("✅ Project structure created")
    
    def install_python_dependencies(self):
        """Install Python dependencies"""
        print("📦 Installing Python dependencies...")
        
        requirements = [
            "Flask==2.3.3",
            "Flask-SocketIO==5.3.6", 
            "Flask-CORS==4.0.0",
            "paho-mqtt==1.6.1",
            "SQLAlchemy==2.0.23",
            "pandas==2.1.3",
            "numpy==1.24.3",
            "scipy==1.11.4",
            "scikit-learn==1.3.2",
            "python-dotenv==1.0.0",
            "requests==2.31.0",
            "python-dateutil==2.8.2",
            "PyYAML==6.0.1",
            "psutil==5.9.6"
        ]
        
        try:
            for requirement in requirements:
                print(f"  Installing {requirement}...")
                subprocess.run([
                    self.python_executable, "-m", "pip", "install", requirement
                ], check=True, capture_output=True)
                print(f"  ✅ {requirement}")
            
            print("✅ All Python dependencies installed")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install dependencies: {e}")
            return False
    
    def check_mqtt_broker(self):
        """Check and install MQTT broker if needed"""
        print("🔌 Checking MQTT broker...")
        
        try:
            # Try to find mosquitto
            subprocess.run(["mosquitto", "--help"], 
                         capture_output=True, check=True)
            print("✅ Mosquitto MQTT broker found")
            return True
            
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("⚠️ Mosquitto not found. Installing...")
            return self.install_mqtt_broker()
    
    def install_mqtt_broker(self):
        """Install MQTT broker based on OS"""
        try:
            if self.system_os == "Linux":
                # Ubuntu/Debian
                subprocess.run([
                    "sudo", "apt-get", "update"
                ], check=True)
                subprocess.run([
                    "sudo", "apt-get", "install", "-y", "mosquitto", "mosquitto-clients"
                ], check=True)
                
            elif self.system_os == "Darwin":  # macOS
                # Check if Homebrew is installed
                try:
                    subprocess.run(["brew", "--version"], 
                                 capture_output=True, check=True)
                    subprocess.run([
                        "brew", "install", "mosquitto"
                    ], check=True)
                except FileNotFoundError:
                    print("❌ Homebrew not found. Please install manually:")
                    print("   Visit: https://brew.sh")
                    return False
                    
            elif self.system_os == "Windows":
                print("⚠️ Windows detected. Please install manually:")
                print("   1. Download from: https://mosquitto.org/download/")
                print("   2. Or use Chocolatey: choco install mosquitto")
                return False
            
            print("✅ MQTT broker installed")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install MQTT broker: {e}")
            print("💡 You can continue without MQTT - system will work in simulation mode")
            return False
    
    def create_config_files(self):
        """Create configuration files"""
        print("⚙️ Creating configuration files...")
        
        # Create .env file
        env_content = """# Smart HVAC Environment Configuration
FLASK_ENV=development
SECRET_KEY=smart-hvac-secret-key-2024
MQTT_BROKER=localhost
MQTT_PORT=1883
DATABASE_URL=sqlite:///database/hvac_data.db
LOG_LEVEL=INFO
"""
        
        env_file = self.project_root / '.env'
        with open(env_file, 'w') as f:
            f.write(env_content)
        print("  ✅ .env file created")
        
        # Create requirements.txt
        requirements_content = """# Smart HVAC Monitoring System Dependencies
Flask==2.3.3
Flask-SocketIO==5.3.6
Flask-CORS==4.0.0
paho-mqtt==1.6.1
SQLAlchemy==2.0.23
pandas==2.1.3
numpy==1.24.3
scipy==1.11.4
scikit-learn==1.3.2
python-dotenv==1.0.0
requests==2.31.0
python-dateutil==2.8.2
PyYAML==6.0.1
psutil==5.9.6

# Development dependencies
pytest==7.4.3
pytest-flask==1.3.0
black==23.11.0
flake8==6.1.0
"""
        
        req_file = self.project_root / 'requirements.txt'
        with open(req_file, 'w') as f:
            f.write(requirements_content)
        print("  ✅ requirements.txt created")
        
        # Create gitignore
        gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual Environment
venv/
env/
ENV/
env.bak/
venv.bak/

# IDEs
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Project specific
.env
*.log
logs/
database/*.db
database/*.db-journal

# Testing
.pytest_cache/
.coverage
htmlcov/

# Documentation
docs/_build/
"""
        
        gitignore_file = self.project_root / '.gitignore'
        with open(gitignore_file, 'w') as f:
            f.write(gitignore_content)
        print("  ✅ .gitignore created")
        
        print("✅ Configuration files created")
    
    def initialize_database(self):
        """Initialize the database"""
        print("🗄️ Initializing database...")
        
        try:
            # Run database initialization
            init_script = self.project_root / 'database' / 'init_db.py'
            subprocess.run([
                self.python_executable, str(init_script)
            ], check=True, cwd=self.project_root)
            
            print("✅ Database initialized")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Database initialization failed: {e}")
            return False
    
    def create_startup_scripts(self):
        """Create convenient startup scripts"""
        print("📝 Creating startup scripts...")
        
        # Windows batch file
        if self.system_os == "Windows":
            batch_content = """@echo off
echo Starting Smart HVAC Monitoring System...
python scripts/start_system.py
pause
"""
            batch_file = self.project_root / 'start_hvac.bat'
            with open(batch_file, 'w') as f:
                f.write(batch_content)
            print("  ✅ start_hvac.bat created")
        
        # Unix shell script
        else:
            shell_content = """#!/bin/bash
echo "Starting Smart HVAC Monitoring System..."
python3 scripts/start_system.py
"""
            shell_file = self.project_root / 'start_hvac.sh'
            with open(shell_file, 'w') as f:
                f.write(shell_content)
            
            # Make executable
            os.chmod(shell_file, 0o755)
            print("  ✅ start_hvac.sh created")
        
        print("✅ Startup scripts created")
    
    def verify_installation(self):
        """Verify the installation"""
        print("🔍 Verifying installation...")
        
        checks = []
        
        # Check Python imports
        try:
            import flask
            import paho.mqtt.client
            import sqlalchemy
            import pandas
            import numpy
            import sklearn
            checks.append("✅ Python dependencies")
        except ImportError as e:
            checks.append(f"❌ Python dependencies: {e}")
        
        # Check project structure
        required_dirs = ['sensors', 'database', 'web_dashboard', 'config']
        missing_dirs = []
        for dir_name in required_dirs:
            if not (self.project_root / dir_name).exists():
                missing_dirs.append(dir_name)
        
        if not missing_dirs:
            checks.append("✅ Project structure")
        else:
            checks.append(f"❌ Missing directories: {missing_dirs}")
        
        # Check database
        if (self.project_root / 'database' / 'hvac_data.db').exists():
            checks.append("✅ Database")
        else:
            checks.append("⚠️ Database not initialized")
        
        # Check configuration
        if (self.project_root / '.env').exists():
            checks.append("✅ Configuration files")
        else:
            checks.append("❌ Configuration files missing")
        
        print("\nInstallation Status:")
        for check in checks:
            print(f"  {check}")
        
        success_count = sum(1 for check in checks if check.startswith("✅"))
        total_checks = len(checks)
        
        if success_count == total_checks:
            print(f"\n🎉 Installation completed successfully! ({success_count}/{total_checks})")
            return True
        else:
            print(f"\n⚠️ Installation completed with issues ({success_count}/{total_checks})")
            return False
    
    def run_setup(self):
        """Run the complete setup process"""
        print("🚀 Starting Smart HVAC system setup...\n")
        
        steps = [
            ("Check Python version", self.check_python_version),
            ("Create project structure", self.create_project_structure),
            ("Install Python dependencies", self.install_python_dependencies),
            ("Check MQTT broker", self.check_mqtt_broker),
            ("Create configuration files", self.create_config_files),
            ("Initialize database", self.initialize_database),
            ("Create startup scripts", self.create_startup_scripts),
            ("Verify installation", self.verify_installation)
        ]
        
        for step_name, step_func in steps:
            print(f"\n🔄 {step_name}...")
            try:
                if not step_func():
                    print(f"⚠️ {step_name} completed with warnings")
            except Exception as e:
                print(f"❌ {step_name} failed: {e}")
                response = input("Continue anyway? (y/N): ").lower()
                if response != 'y':
                    print("Setup aborted.")
                    return False
        
        self.show_completion_message()
        return True
    
    def show_completion_message(self):
        """Show setup completion message"""
        print(f"""
╔══════════════════════════════════════════════════════════════════╗
║  🎉 Smart HVAC Setup Completed Successfully!                    ║
║                                                                  ║
║  Next Steps:                                                     ║
║  1. Start the system:                                            ║
║     • Windows: double-click start_hvac.bat                       ║
║     • Linux/Mac: ./start_hvac.sh                                 ║
║     • Or: python scripts/start_system.py                        ║
║                                                                  ║
║  2. Access the dashboard:                                        ║
║     • http://localhost:5000                                      ║
║                                                                  ║
║  3. Explore the features:                                        ║
║     • Real-time monitoring                                       ║
║     • Interactive charts                                         ║
║     • Smart alerts                                               ║
║     • Building map                                               ║
║                                                                  ║
║  📖 Documentation: README.md                                     ║
║  🐛 Issues: GitHub Issues                                        ║
║  💬 Support: Create a discussion                                 ║
╚══════════════════════════════════════════════════════════════════╝
        """)

def main():
    """Main setup function"""
    setup = SmartHVACSetup()
    
    try:
        success = setup.run_setup()
        if success:
            launch = input("\n🚀 Would you like to start the system now? (y/N): ").lower()
            if launch == 'y':
                print("\n🌟 Launching Smart HVAC system...")
                subprocess.run([
                    setup.python_executable, 
                    str(setup.project_root / 'scripts' / 'start_system.py')
                ])
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error during setup: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()