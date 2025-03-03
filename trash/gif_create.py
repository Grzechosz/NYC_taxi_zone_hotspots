from PIL import Image

def create_gif(output_filename="merged_data_maps.gif", duration=400):
    image_files = [f"merged_data_maps/map_hour_{i}.png" for i in range(24)]
    images = []
    
    for file in image_files:
        try:
            images.append(Image.open(file))
        except FileNotFoundError:
            print(f"Warning: {file} not found. Skipping...")
    
    if images:
        images[0].save(output_filename, save_all=True, append_images=images[1:], duration=duration, loop=0)
        print(f"GIF saved as {output_filename}")
    else:
        print("No images found. GIF not created.")

if __name__ == "__main__":
    create_gif()