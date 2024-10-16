import imageio
import os

# Directory containing the PNG files
image_dir = "/home/jmframe/CIROH_DL_NextGen/tools/ngen/vpu_plots"

# Output path for the GIF
gif_output_path = "conus_routing.gif"

# Get all PNG files and sort them numerically by timestep
image_files = sorted(
    [f for f in os.listdir(image_dir) if f.startswith("conus_routing_timestep_") and f.endswith(".png")],
    key=lambda x: int(x.split('_')[-1].split('.')[0])  # Extract the numeric timestep for sorting
)

# Create the GIF
with imageio.get_writer(gif_output_path, mode="I", duration=0.2) as writer:
    for filename in image_files:
        file_path = os.path.join(image_dir, filename)
        image = imageio.imread(file_path)
        writer.append_data(image)

print(f"GIF created successfully at: {gif_output_path}")