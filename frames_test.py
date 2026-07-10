import pyrealsense2 as rs
import numpy as np
import cv2

def main():
    # Initialize the RealSense pipeline
    pipeline = rs.pipeline()
    config = rs.config()

    # Request the NATIVE 1280x800 resolution
    config.enable_stream(rs.stream.color, 1280, 800, rs.format.bgr8, 30)

    print("Starting camera pipeline...")
    pipeline.start(config)

    try:
        while True:
            # Wait for a coherent color frame
            frames = pipeline.wait_for_frames()
            color_frame = frames.get_color_frame()
            if not color_frame:
                continue

            # Convert RealSense frame to numpy array for OpenCV
            img = np.asanyarray(color_frame.get_data())

            # --- 1. Define Tighter Crop Boundaries ---
            crop_x1 = 220   
            crop_x2 = 1030  
            crop_y1 = 0     
            crop_y2 = 800   

            # Crop the image
            cropped_img = img[crop_y1:crop_y2, crop_x1:crop_x2]
            h, w = cropped_img.shape[:2]

            # --- 2. Draw the Adjusted Grid ---
            grid_color = (0, 0, 255) # Red lines
            thickness = 3

            # Define NEW horizontal boundaries for C1 and C2
            # Adjusted to make C1 and C2 much narrower
            line_c1_y = int(h * 0.08)  # Moved UP to top 8% of the frame
            line_c2_y = int(h * 0.92)  # Moved DOWN to bottom 92% of the frame
            mid_y = h // 2             # Divides A and B

            # Draw horizontal lines spanning the full width
            cv2.line(cropped_img, (0, line_c1_y), (w, line_c1_y), grid_color, thickness) # C1 boundary
            cv2.line(cropped_img, (0, mid_y), (w, mid_y), grid_color, thickness)         # A/B boundary
            cv2.line(cropped_img, (0, line_c2_y), (w, line_c2_y), grid_color, thickness) # C2 boundary

            # --- ADJUSTABLE VERTICAL DIVIDERS ---
            line_1_x = int(w * 0.34)  
            line_2_x = int(w * 0.65)  
            
            # Vertical lines span between C1 and C2
            cv2.line(cropped_img, (line_1_x, line_c1_y), (line_1_x, line_c2_y), grid_color, thickness)
            cv2.line(cropped_img, (line_2_x, line_c1_y), (line_2_x, line_c2_y), grid_color, thickness)

            # --- 3. Draw Section Labels ---
            font = cv2.FONT_HERSHEY_SIMPLEX
            font_scale = 1.2
            font_color = (0, 255, 255) # Yellow
            font_thickness = 3

            mid_lane_1 = line_1_x // 2
            mid_lane_2 = (line_1_x + line_2_x) // 2
            mid_lane_3 = (line_2_x + w) // 2

            txt_offset = 30 
            
            # Calculate dynamic Y positions for A and B to stay centered in their newly expanded boxes
            a_y = int(line_c1_y + (mid_y - line_c1_y) * 0.5)
            b_y = int(mid_y + (line_c2_y - mid_y) * 0.5)

            label_positions = {
                "C1": (w // 2 - txt_offset, int(line_c1_y * 0.7)), # Tightly centered in the top section
                "A1": (mid_lane_1 - txt_offset, a_y),
                "A2": (mid_lane_2 - txt_offset, a_y),
                "A3": (mid_lane_3 - txt_offset, a_y),
                "B1": (mid_lane_1 - txt_offset, b_y),
                "B2": (mid_lane_2 - txt_offset, b_y),
                "B3": (mid_lane_3 - txt_offset, b_y),
                "C2": (w // 2 - txt_offset, int(line_c2_y + (h - line_c2_y) * 0.7)) # Tightly centered in the bottom section
            }

            for label, pos in label_positions.items():
                cv2.putText(cropped_img, label, pos, font, font_scale, font_color, font_thickness, cv2.LINE_AA)

            # --- 4. Draw Belt Direction Indicator (Vertical) ---
            arrow_x = w - 40
            cv2.arrowedLine(cropped_img, (arrow_x, int(h * 0.2)), (arrow_x, int(h * 0.8)), (255, 255, 0), 4, tipLength=0.03)

            vert_text = "BELT TRAVEL"
            start_y = int(h * 0.3)
            for i, char in enumerate(vert_text):
                cv2.putText(cropped_img, char, (arrow_x - 20, start_y + i * 25), font, 0.7, (255, 255, 255), 2, cv2.LINE_AA)

            # Show the final cropped and gridded image
            cv2.imshow('D455 Sorting Frame', cropped_img)

            # Press 'q' to close the window and exit
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

    finally:
        print("Stopping pipeline...")
        pipeline.stop()
        cv2.destroyAllWindows()

if __name__ == "__main__":
    main()