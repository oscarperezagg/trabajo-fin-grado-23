import platform
import subprocess

def is_wifi_connected():
    system = platform.system()
    if system == "Windows":
        try:
            output = subprocess.check_output(["netsh", "interface", "show", "interface"])
            return b"Wi-Fi" in output
        except subprocess.CalledProcessError:
            return False
    elif system == "Darwin":
        try:
            output = subprocess.check_output(["networksetup", "-listallhardwareports"])
            return b"Hardware Port: Wi-Fi" in output
        except subprocess.CalledProcessError:
            return False
    elif system == "Linux":
        try:
            output = subprocess.check_output(["iwconfig"])
            return b"Wi-Fi" in output
        except subprocess.CalledProcessError:
            return False
    else:
        print("Unsupported operating system")
        return False

if is_wifi_connected():
    print("Wi-Fi is connected.")
else:
    print("Wi-Fi is not connected.")
    
    


def check_wifi_connection():
    try:
        # Execute a command that returns network status
        result = subprocess.check_output(["/System/Library/PrivateFrameworks/Apple80211.framework/Versions/Current/Resources/airport", "-I"], text=True)

        # Check if the output indicates an active Wi-Fi connection
        if "AirPort: Off" in result:
            return False
        else:
            return True
    except subprocess.CalledProcessError:
        # Handle errors (e.g., command not found)
        return False

# Check Wi-Fi connection
is_connected = check_wifi_connection()
print("Wi-Fi Connected:", is_connected)

