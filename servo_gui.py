import time
import threading
import tkinter as tk
from tkinter import ttk
from adafruit_servokit import ServoKit

# ==========================================
# HARDWARE INITIALIZATION
# ==========================================
print("Initializing PCA9685 on I2C bus 1...")
try:
    kit = ServoKit(channels=16)
    
    # --- PULSE WIDTH CALIBRATION ---
    # This expands the PWM signal to ensure the servos reach their physical limits
    # with maximum holding force. (Adjust 500/2500 if your servo datasheet differs).
    for i in range(16):
        kit.servo[i].set_pulse_width_range(500, 2500)
        
    hardware_active = True
    print("Hardware initialized successfully with calibrated pulse widths.")
except Exception as e:
    print(f"Hardware Error: {e}. Running GUI in offline mode.")
    hardware_active = False

motor_map = {
    "A1": 0, "A2": 1, "A3": 2,
    "B1": 3, "B2": 4, "B3": 5
}

POSITIONS = {
    "Reject": 0,
    "Home": 85,
    "Accept": 180
}

ACTION_OPTIONS = ["Skip", "Reject", "Home", "Accept"]

# ==========================================
# GUI APPLICATION (Native Dark Mode)
# ==========================================
class SAATControllerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        
        self.title("SAAT System - Sorting Controller")
        self.geometry("800x650")
        
        # Color Palette
        self.bg_main = "#1e1e1e"
        self.bg_card = "#2b2d30"
        self.fg_text = "#ffffff"
        self.accent = "#34a853" # Industrial Green
        self.accent_hover = "#2b8a44"
        
        self.configure(bg=self.bg_main)
        self.setup_styles()
        
        # Main Tabview
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(padx=20, pady=20, fill="both", expand=True)
        
        self.tab1 = ttk.Frame(self.notebook, style="Main.TFrame")
        self.tab2 = ttk.Frame(self.notebook, style="Main.TFrame")
        
        self.notebook.add(self.tab1, text='  Manual Control  ')
        self.notebook.add(self.tab2, text='  Sequence Execution  ')
        
        self.build_manual_tab()
        self.build_sequence_tab()

    def setup_styles(self):
        style = ttk.Style(self)
        # 'clam' theme allows changing background colors on Linux natively
        style.theme_use('clam')
        
        # Frames
        style.configure("Main.TFrame", background=self.bg_main)
        style.configure("Card.TFrame", background=self.bg_card)
        
        # Labels
        style.configure("TLabel", background=self.bg_main, foreground=self.fg_text, font=("Arial", 11))
        style.configure("Card.TLabel", background=self.bg_card, foreground=self.fg_text, font=("Arial", 11))
        style.configure("Header.TLabel", background=self.bg_card, foreground=self.fg_text, font=("Arial", 14, "bold"))
        
        # Buttons
        style.configure("Action.TButton", background=self.accent, foreground="white", font=("Arial", 12, "bold"), padding=10, borderwidth=0)
        style.map("Action.TButton", background=[('active', self.accent_hover)])
        
        # Notebook (Tabs)
        style.configure("TNotebook", background=self.bg_main, borderwidth=0)
        style.configure("TNotebook.Tab", background=self.bg_card, foreground=self.fg_text, font=("Arial", 12, "bold"), padding=[10, 5], borderwidth=0)
        style.map("TNotebook.Tab", background=[("selected", self.accent)])
        
        # RadioButtons
        style.configure("TRadiobutton", background=self.bg_card, foreground=self.fg_text, font=("Arial", 11))
        style.map("TRadiobutton", background=[('active', self.bg_card)], indicatorcolor=[('selected', self.accent)])
        
        # Combobox / Dropdowns
        style.configure("TCombobox", fieldbackground=self.bg_main, background=self.bg_card, foreground=self.fg_text, borderwidth=0)

    # ----------------------------------------
    # TAB 1: MANUAL CONTROL GRID
    # ----------------------------------------
    def build_manual_tab(self):
        self.manual_vars = {}
        
        grid_frame = ttk.Frame(self.tab1, style="Main.TFrame")
        grid_frame.pack(pady=20, expand=True)

        top_motors = ["A1", "A2", "A3"]
        for col, motor in enumerate(top_motors):
            self.create_motor_card(grid_frame, motor, 0, col)
            
        bottom_motors = ["B1", "B2", "B3"]
        for col, motor in enumerate(bottom_motors):
            self.create_motor_card(grid_frame, motor, 1, col)
            
        exec_btn = ttk.Button(self.tab1, text="EXECUTE MANUAL POSITIONS", style="Action.TButton", command=self.execute_manual)
        exec_btn.pack(pady=20, padx=40, fill="x")

    def create_motor_card(self, parent, motor_name, row, col):
        card = ttk.Frame(parent, style="Card.TFrame")
        # ipadx and ipady simulate padding to create the visual "card" effect
        card.grid(row=row, column=col, padx=15, pady=15, ipadx=15, ipady=10, sticky="nsew")
        
        ttk.Label(card, text=motor_name, style="Header.TLabel").pack(pady=(10, 5))
        
        var = tk.IntVar(value=POSITIONS["Home"])
        self.manual_vars[motor_name] = var
        
        ttk.Radiobutton(card, text="Reject (0Â°)", variable=var, value=POSITIONS["Reject"]).pack(pady=5, padx=20, anchor="w")
        ttk.Radiobutton(card, text="Home (85Â°)", variable=var, value=POSITIONS["Home"]).pack(pady=5, padx=20, anchor="w")
        ttk.Radiobutton(card, text="Accept (180Â°)", variable=var, value=POSITIONS["Accept"]).pack(pady=(5, 10), padx=20, anchor="w")

    def execute_manual(self):
        print("\n--- Executing Manual State ---")
        for motor, var in self.manual_vars.items():
            self.move_motor(motor, var.get())

    # ----------------------------------------
    # TAB 2: SEQUENCE BUILDER
    # ----------------------------------------
    def build_sequence_tab(self):
        self.step1_actions = {m: tk.StringVar(value="Skip") for m in motor_map.keys()}
        self.step2_actions = {m: tk.StringVar(value="Skip") for m in motor_map.keys()}
        self.delay_var = tk.DoubleVar(value=1.0)
        
        self.create_sequence_card(self.tab2, "Step 1 Actions", self.step1_actions).pack(fill="x", pady=(15, 5), padx=20)
        
        delay_frame = ttk.Frame(self.tab2, style="Main.TFrame")
        delay_frame.pack(pady=10)
        
        ttk.Label(delay_frame, text="Delay Between Steps (seconds):", style="TLabel").pack(side="left", padx=10)
        delay_entry = ttk.Entry(delay_frame, textvariable=self.delay_var, width=8, font=("Arial", 12))
        delay_entry.pack(side="left")
        
        self.create_sequence_card(self.tab2, "Step 2 Actions", self.step2_actions).pack(fill="x", pady=(5, 15), padx=20)
        
        run_btn = ttk.Button(self.tab2, text="RUN SEQUENCE", style="Action.TButton", command=self.start_sequence)
        run_btn.pack(pady=20, padx=40, fill="x")

    def create_sequence_card(self, parent, title, action_dict):
        card = ttk.Frame(parent, style="Card.TFrame")
        
        ttk.Label(card, text=title, style="Header.TLabel").pack(pady=(15, 5))
        
        grid_frame = ttk.Frame(card, style="Card.TFrame")
        grid_frame.pack(pady=10, padx=20, expand=True)
        
        motors_list = list(motor_map.keys())
        for i, motor in enumerate(motors_list):
            row = i // 3
            col = i % 3
            
            sub_frame = ttk.Frame(grid_frame, style="Card.TFrame")
            sub_frame.grid(row=row, column=col, padx=15, pady=10)
            
            ttk.Label(sub_frame, text=motor, style="Card.TLabel").pack(side="left", padx=(0, 10))
            dropdown = ttk.Combobox(sub_frame, textvariable=action_dict[motor], values=ACTION_OPTIONS, state="readonly", width=8, font=("Arial", 10))
            dropdown.pack(side="left")
            
        return card

    def start_sequence(self):
        # We keep threading here so the time.sleep() doesn't freeze the tkinter GUI
        threading.Thread(target=self.execute_sequence, daemon=True).start()

    def execute_sequence(self):
        print("\n=== Starting Sequence ===")
        
        print("-> Executing Step 1")
        for motor, action_var in self.step1_actions.items():
            action = action_var.get()
            if action != "Skip":
                self.move_motor(motor, POSITIONS[action])
                
        try:
            delay = self.delay_var.get()
        except tk.TclError:
            delay = 1.0 # Fallback
            
        print(f"-> Waiting for {delay} seconds...")
        time.sleep(delay)
        
        print("-> Executing Step 2")
        for motor, action_var in self.step2_actions.items():
            action = action_var.get()
            if action != "Skip":
                self.move_motor(motor, POSITIONS[action])
                
        print("=== Sequence Complete ===\n")

    # ----------------------------------------
    # HARDWARE COMMAND WRAPPER
    # ----------------------------------------
    def move_motor(self, motor_name, angle):
        channel = motor_map[motor_name]
        print(f"  Setting {motor_name} (Ch {channel}) to {angle}Â°")
        if hardware_active:
            kit.servo[channel].angle = angle

if __name__ == "__main__":
    app = SAATControllerApp()
    app.mainloop()