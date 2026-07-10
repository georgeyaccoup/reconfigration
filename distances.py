import pyrealsense2 as rs
import numpy as np
import cv2
import os

# --- GLOBAL VARIABLES ---
latest_distances = None
save_counter = 1

# --- HELPER FUNCTION: Rescaled Adjustable Grid ---
def draw_grid(img):
    h, w = img.shape[:2]
    grid_color = (0, 0, 255) # Red lines
    thickness = 3

    # ==========================================
    # ð ï¸ TUNE THESE PERCENTAGES TO FIT YOUR RIG
    # Adjust these slightly (e.g., 0.31 to 0.32) to snap 
    # the red lines perfectly onto the physical black frame!
    # ==========================================
    top_margin_pct = 0.08    # Top horizontal line (C1 border)
    mid_y_pct = 0.52         # Center horizontal line (A/B border)
    bottom_margin_pct = 0.95 # Bottom horizontal line (C2 border)
    
    vert_left_pct = 0.30     # Left vertical line (A1/A2 border)
    vert_right_pct = 0.63    # Right vertical line (A2/A3 border)
    # ==========================================

    # Calculate actual pixel positions based on tuned percentages
    line_c1_y = int(h * top_margin_pct)  
    mid_y = int(h * mid_y_pct)             
    line_c2_y = int(h * bottom_margin_pct)  

    line_1_x = int(w * vert_left_pct)  
    line_2_x = int(w * vert_right_pct)  

    # Draw Horizontal Boundaries
    cv2.line(img, (0, line_c1_y), (w, line_c1_y), grid_color, thickness) 
    cv2.line(img, (0, mid_y), (w, mid_y), grid_color, thickness)         
    cv2.line(img, (0, line_c2_y), (w, line_c2_y), grid_color, thickness) 

    # Draw Vertical Boundaries
    cv2.line(img, (line_1_x, line_c1_y), (line_1_x, line_c2_y), grid_color, thickness)
    cv2.line(img, (line_2_x, line_c1_y), (line_2_x, line_c2_y), grid_color, thickness)

    # Section Labels
    font = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = 1.2
    font_color = (0, 255, 255) # Yellow
    font_thickness = 3
    txt_offset = 30 

    mid_lane_1 = line_1_x // 2
    mid_lane_2 = (line_1_x + line_2_x) // 2
    mid_lane_3 = (line_2_x + w) // 2
    
    a_y = int(line_c1_y + (mid_y - line_c1_y) * 0.5)
    b_y = int(mid_y + (line_c2_y - mid_y) * 0.5)

    label_positions = {
        "C1": (w // 2 - txt_offset, int(line_c1_y * 0.7)), 
        "A1": (mid_lane_1 - txt_offset, a_y),
        "A2": (mid_lane_2 - txt_offset, a_y),
        "A3": (mid_lane_3 - txt_offset, a_y),
        "B1": (mid_lane_1 - txt_offset, b_y),
        "B2": (mid_lane_2 - txt_offset, b_y),
        "B3": (mid_lane_3 - txt_offset, b_y),
        "C2": (w // 2 - txt_offset, int(line_c2_y + (h - line_c2_y) * 0.7)) 
    }

    for label, pos in label_positions.items():
        cv2.putText(img, label, pos, font, font_scale, font_color, font_thickness, cv2.LINE_AA)

    # Belt Direction Indicator
    arrow_x = w - 40
    cv2.arrowedLine(img, (arrow_x, int(h * 0.2)), (arrow_x, int(h * 0.8)), (255, 255, 0), 4, tipLength=0.03)
    vert_text = "BELT TRAVEL"
    start_y = int(h * 0.3)
    for i, char in enumerate(vert_text):
        cv2.putText(img, char, (arrow_x - 20, start_y + i * 25), font, 0.7, (255, 255, 255), 2, cv2.LINE_AA)

# --- MOUSE CLICK FUNCTION ---
def save_excel_on_click(event, x, y, flags, param):
    global latest_distances, save_counter
    if event == cv2.EVENT_LBUTTONDOWN:
        if latest_distances is not None:
            print("\n--- MOUSE CLICK DETECTED ---")
            filename = f"pear_distances_grid_{save_counter}.csv"
            save_path = os.path.join(os.getcwd(), filename)
            
            # Save the cropped distance array directly to CSV
            np.savetxt(save_path, latest_distances, delimiter=",", fmt="%.4f")
            
            print(f"â EXCEL/CSV SAVED SUCCESSFULLY!")
            print(f"ð Location: {save_path}")
            print("----------------------------\n")
            save_counter += 1

def main():
    global latest_distances
    
    print("Initializing RealSense D455...")
    pipeline = rs.pipeline()
    config = rs.config()

    # High resolution
    config.enable_stream(rs.stream.color, 1280, 800, rs.format.bgr8, 30)
    config.enable_stream(rs.stream.depth, 1280, 720, rs.format.z16, 30)

    profile = pipeline.start(config)
    depth_scale = profile.get_device().first_depth_sensor().get_depth_scale()

    # --- INITIALIZE REAL-SENSE FILTERS ---
    spatial_filter = rs.spatial_filter()
    temporal_filter = rs.temporal_filter()
    hole_filling = rs.hole_filling_filter()

    align = rs.align(rs.stream.color)
    colorizer = rs.colorizer()

    window_color = 'Normal Color View'
    window_depth = 'Colorized Depth View'
    cv2.namedWindow(window_color)
    cv2.namedWindow(window_depth)
    
    cv2.setMouseCallback(window_color, save_excel_on_click)
    cv2.setMouseCallback(window_depth, save_excel_on_click)

    print("\n--- READY ---")
    print(">>> Click your mouse ANYWHERE inside EITHER window to save the Excel sheet. <<<")
    print(">>> Press 'q' in the terminal or on a window to quit. <<<")
    print("-------------\n")

    try:
        while True:
            frames = pipeline.wait_for_frames()
            
            # 1. Align the frames
            aligned_frames = align.process(frames)
            depth_frame = aligned_frames.get_depth_frame()
            color_frame = aligned_frames.get_color_frame()
            
            if not depth_frame or not color_frame:
                continue

            # 2. APPLY THE FILTERS TO THE DEPTH FRAME
            # (Fills in the black holes on the foam)
            depth_frame = spatial_filter.process(depth_frame)
            depth_frame = temporal_filter.process(depth_frame)
            depth_frame = hole_filling.process(depth_frame)

            color_image = np.asanyarray(color_frame.get_data())
            raw_depth_image = np.asanyarray(depth_frame.get_data())
            depth_colormap_full = np.asanyarray(colorizer.colorize(depth_frame).get_data())

            # --- Shifted Crop Boundaries ---
            # I widened the left side slightly (from 220 down to 180) to help 
            # stop the A1 and B1 compartments from getting chopped off!
            crop_x1 = 180   
            crop_x2 = 1030  
            crop_y1 = 0     
            crop_y2 = 800   

            # Crop all arrays
            cropped_color = color_image[crop_y1:crop_y2, crop_x1:crop_x2].copy()
            cropped_depth_raw = raw_depth_image[crop_y1:crop_y2, crop_x1:crop_x2]
            cropped_depth_colored = depth_colormap_full[crop_y1:crop_y2, crop_x1:crop_x2].copy()

            # Store the raw distances (in meters)
            latest_distances = cropped_depth_raw * depth_scale

            # --- DRAW THE GRIDS ---
            draw_grid(cropped_color)
            draw_grid(cropped_depth_colored)

            # Show the windows
            cv2.imshow(window_color, cropped_color)
            cv2.imshow(window_depth, cropped_depth_colored)

            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

    finally:
        pipeline.stop()
        cv2.destroyAllWindows()
        print("Pipeline stopped.")

if __name__ == "__main__":
    main()