import Jetson.GPIO as GPIO
import time
import threading

# --- Setup Configuration ---
PIN = 11
MAX_VOLTAGE = 3.3
FREQUENCY = 500  # How fast the pin turns on/off (500 times per second)
PERIOD = 1.0 / FREQUENCY

# Shared variables between the input menu and the background hardware worker
running = True
current_duty_cycle = 0.0  # Starts at 0 Volts

def generate_pwm():
    """
    This is the background worker. 
    Its ONLY job is to rapidly turn Pin 11 ON and OFF to create the voltage.
    """
    while running:
        if current_duty_cycle <= 0.0:
            # If 0V is requested, keep the pin OFF completely
            GPIO.output(PIN, GPIO.LOW)
            time.sleep(0.01)
        elif current_duty_cycle >= 1.0:
            # If 3.3V is requested, keep the pin ON completely
            GPIO.output(PIN, GPIO.HIGH)
            time.sleep(0.01)
        else:
            # Calculate exactly how long to stay ON and OFF
            time_on = PERIOD * current_duty_cycle
            time_off = PERIOD * (1.0 - current_duty_cycle)
            
            # Turn ON
            GPIO.output(PIN, GPIO.HIGH)
            time.sleep(time_on)
            
            # Turn OFF
            GPIO.output(PIN, GPIO.LOW)
            time.sleep(time_off)

def main():
    global running, current_duty_cycle
    
    # Configure Jetson Nano Pin 11
    GPIO.setwarnings(False)
    GPIO.setmode(GPIO.BOARD)
    GPIO.setup(PIN, GPIO.OUT)
    
    # Start the background hardware worker
    pwm_thread = threading.Thread(target=generate_pwm)
    pwm_thread.daemon = True
    pwm_thread.start()
    
    print("\n=========================================")
    print(f" Software PWM Voltage Generator Active")
    print(f" Output Pin: {PIN} | Max Voltage: {MAX_VOLTAGE}V")
    print("=========================================")
    
    try:
        # The main menu loop
        while True:
            # Ask the user for the voltage
            user_input = input(f"\nEnter the voltage you want (0 to {MAX_VOLTAGE}), or 'q' to quit: ")
            
            if user_input.lower() == 'q':
                break
                
            try:
                requested_voltage = float(user_input)
                
                # Check if the voltage is physically possible
                if 0.0 <= requested_voltage <= MAX_VOLTAGE:
                    # Convert the requested voltage into a percentage (Duty Cycle)
                    current_duty_cycle = requested_voltage / MAX_VOLTAGE
                    print(f"--> Success! Changing output to ~{requested_voltage}V")
                else:
                    print(f"--> Error: Please enter a number between 0 and {MAX_VOLTAGE}")
                    
            except ValueError:
                print("--> Error: That is not a valid number. Try again.")
                
    except KeyboardInterrupt:
        pass # Handle Ctrl+C gracefully
        
    finally:
        # Safely turn everything off when you quit
        print("\nShutting down safely...")
        running = False
        pwm_thread.join(timeout=1.0) # Wait for the background worker to stop
        GPIO.cleanup()               # Release the pin
        print("Hardware disconnected. Goodbye!")

if __name__ == '__main__':
    main()