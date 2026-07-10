import time
from adafruit_servokit import ServoKit

# Initialize the PCA9685 on I2C bus 1 (Pins 3 and 5 on Jetson Nano)
# The PCA9685 has 16 channels
kit = ServoKit(channels=16)

# Map your motor names to the specific channels on the PCA9685 board
# Change these numbers if you plugged them into different pins (0-15)
motor_map = {
    "A1": 0,
    "A2": 1,
    "A3": 2,
    "B1": 3,
    "B2": 4,
    "B3": 5
}

print("=== Jetson Nano PCA9685 Servo Controller ===")
print("Available motors:", ", ".join(motor_map.keys()))
print("Type 'exit' or 'quit' at any time to stop.\n")

while True:
    # 1. Get the motor names
    motor_input = input("Which motor(s) you need to drive (e.g., A1 or A1 A2 A3): ").strip().upper()
    
    if motor_input in ['EXIT', 'QUIT']:
        print("Exiting...")
        break
    if not motor_input:
        continue
        
    # Replace commas with spaces to allow both "A1,A2" and "A1 A2", then split into a list
    motor_list = motor_input.replace(',', ' ').split()
    
    # Validate the entered motors
    valid_motors = []
    for m in motor_list:
        if m in motor_map:
            valid_motors.append(m)
        else:
            print(f"  [!] Warning: Motor '{m}' is not recognized. Skipping.")
            
    if not valid_motors:
        print("  [!] No valid motors selected. Please try again.\n")
        continue

    # 2. Get the angles
    angle_input = input("What's the angle(s) (e.g., 90 or 90 45 180): ").strip()
    
    if angle_input.upper() in ['EXIT', 'QUIT']:
        print("Exiting...")
        break
        
    # Parse the angles into floats
    try:
        angle_list = [float(a) for a in angle_input.replace(',', ' ').split()]
    except ValueError:
        print("  [!] Error: Please enter numbers only for angles.\n")
        continue

    # 3. Apply the logic
    try:
        # Case A: One angle provided -> apply to all selected motors
        if len(angle_list) == 1:
            target_angle = angle_list[0]
            for motor in valid_motors:
                channel = motor_map[motor]
                kit.servo[channel].angle = target_angle
                print(f"  -> Set {motor} (Channel {channel}) to {target_angle} degrees.")
                
        # Case B: Multiple angles provided -> map them one-to-one with the motors
        elif len(angle_list) == len(valid_motors):
            for motor, angle in zip(valid_motors, angle_list):
                channel = motor_map[motor]
                kit.servo[channel].angle = angle
                print(f"  -> Set {motor} (Channel {channel}) to {angle} degrees.")
                
        # Case C: Mismatch between number of motors and angles
        else:
            print(f"  [!] Error: You gave {len(valid_motors)} motor(s) but {len(angle_list)} angle(s).")
            print("      Please provide either exactly 1 angle, or exactly 1 angle per motor.\n")
            
    except ValueError as e:
        # Most standard servos have a range of 0-180. The library will throw a ValueError if you exceed it.
        print(f"  [!] Hardware Error: {e}. Ensure angles are between 0 and 180.")
        
    print("-" * 40) # Divider for readability