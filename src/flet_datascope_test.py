import flet as ft
import asyncio
import os
import logging
from data_handler import create_dataset_environment, load_data, run_analysis
import data_handler
from data_handler import convert_file_to_csv
from pathlib import Path
import json
import sys

# Global variable to store the DF (SEAN FEATURE BUILDOUT)
current_df = None




# Ensure the current directory is in sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


#SETTINGS!-----------------------------------------------------------------------------------------
SETTINGS_FILE = "dataScope/preferences/theme.json"

def load_theme_preference():
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r") as f:
            data = json.load(f)
            return data.get("dark_mode", False)
    return False

#Dark Mode Save Function
def save_theme_preference(dark_mode: bool):
    with open(SETTINGS_FILE, "w") as f:
        json.dump({"dark_mode": dark_mode}, f)
#SETTINGS!-----------------------------------------------------------------------------------------

# Global flag for checking if dataset was loaded
data_loaded = False
# Indicates if a long operation is running
app_busy = False
# Default chunk size for CSV splitting operations
CHUNK_SIZE_DEFAULT = 256

# File conversion helper variables
convert_input_path = None
convert_output_dir = None

# Icon to represent CSV splitting. Older versions of this file referenced
# ``ft.Icon.SPLIT_CSV_OUTLINE`` which does not exist in Flet.  We gracefully
# fall back to ``HORIZONTAL_SPLIT_OUTLINED`` to avoid runtime errors.
SPLIT_CSV_ICON = getattr(ft.Icons, "SPLIT_CSV_OUTLINE", ft.Icons.HORIZONTAL_SPLIT_OUTLINED)

# Control references
dialog_controls = {
    "output_text_field": None,
    "btn_log": None,
    "btn_data": None,
    "btn_visual": None,
    "status_label": None,
    "file_picker": None,
    "theme_switch": None,
    "chunk_size_input": None,
    "chunk_status": None,
    "logo_image": None,
    "tabs": None,
    "splash_container": None,
    "convert_status": None,
    "convert_file_picker": None,
    "convert_dir_picker": None,
    "convert_file_display": None,
    "convert_dir_display": None,
}

async def write_output(message: str, page: ft.Page):
    print(message)
    if dialog_controls["output_text_field"]:
        dialog_controls["output_text_field"].value += message + "\n"
        page.update()

async def check_data_loaded(page: ft.Page):
    if not data_loaded:
        await write_output("[Error] Load data first before testing.", page)
        return False
    return True

# Flash the logo between grayscale and color when app_busy is True
async def flash_logo(page: ft.Page):
    while True:
        await asyncio.sleep(0.5)
        logo_ref = dialog_controls.get("logo_image")
        logo = logo_ref.current if logo_ref else None
        if not logo:
            continue
        if app_busy:
            logo.color = None if logo.color else ft.Colors.GREY
            logo.update()
        elif logo.color:
            logo.color = None
            logo.update()

def focus_console_tab(page: ft.Page):
    """Switch to the Console tab if the tab control exists."""
    tabs = dialog_controls.get("tabs")
    if tabs:
        tabs.selected_index = 0
        page.update()

async def logging_handler_test(e: ft.ControlEvent):
    page = e.page
    if not await check_data_loaded(page):
        return
    await write_output("[Logging Handler] Test complete: Logging system operational.", page)

async def data_handler_test(e: ft.ControlEvent):
    page = e.page
    if not await check_data_loaded(page):
        return
    await write_output("[Data Handler] Test complete: Data loaded and validated.", page)

async def visual_analyst_test(e: ft.ControlEvent):
    page = e.page
    if not await check_data_loaded(page):
        return
    await write_output("[Visual Analyst] Test complete: Visualizations generated.", page)


#FILE HANDLER BLOCK----------------------------------------------------------------------------------------
from data_handler import save_filepath, get_data_stats, split_into_chunks
from pathlib import Path
#----------------------------------------------------------------------------------------------------------

async def load_data_result(e: ft.FilePickerResultEvent):
    global current_df, data_loaded, app_busy
    page = e.page


    if e.files:
        file_path = e.files[0].path
        save_filepath(file_path)           # ← store the real string path
        dialog_controls["loaded_file"] = file_path

        # 1. Get dataset name from file
        dataset_name = Path(file_path).stem

        # Indicate busy state
        app_busy = True

        # 2. Create environment folders for this dataset
        project_paths = create_dataset_environment(dataset_name)
        await write_output(f"[Environment] Folders created at: {project_paths['project']}", page)

        # 3. Show status
        dialog_controls["status_label"].value = "Loading large file..."
        dialog_controls["status_label"].color = ft.Colors.AMBER
        page.update()
        await asyncio.sleep(0.1)

        # 4. Load the data in background
        df = await asyncio.to_thread(load_data, file_path)
        current_df = df

        # in load_data_result, right after `current_df = df`
            # after current_df = df
        cd = dialog_controls["column_dropdown"]
        cd.options = [ft.dropdown.Option("All Columns")] + [
            ft.dropdown.Option(c) for c in current_df.columns
        ]
        cd.value = "All Columns"   # reset selection
        page.update()




        info = get_data_stats(df, file_path)
        await write_output(info["log1"], page)
        await write_output(info["log2"], page)

        # 4. Toggle buttons now that loading succeeded
        data_loaded = True
        dialog_controls["btn_log"].disabled = False
        dialog_controls["btn_data"].disabled = False
        dialog_controls["btn_visual"].disabled = False
        dialog_controls["status_label"].value = "Ready"
        dialog_controls["status_label"].color = ft.Colors.GREEN
        dialog_controls["status_label"].weight = ft.FontWeight.BOLD
        app_busy = False

    else:
        await write_output("[Load Data] No file selected.", page)
        dialog_controls["status_label"].value = "Ready"
        dialog_controls["status_label"].color = ft.Colors.RED
        app_busy = False


    page.update()


def handle_file_result(e: ft.FilePickerResultEvent):
    if e.files:
        selected_file = e.files[0].path
        dialog_controls["status_label"].value = f"Loaded: {selected_file}"
        dialog_controls["status_label"].color = ft.Colors.GREEN

async def load_data_handler(e: ft.ControlEvent):
    page = e.page
    dialog_controls["status_label"].value = "Waiting..."
    dialog_controls["status_label"].color = ft.Colors.ORANGE

    # ✅ Make sure the file_picker is set up *before* calling pick_files
    if dialog_controls["file_picker"] is None:
        dialog_controls["file_picker"] = ft.FilePicker(on_result=handle_file_result)
        page.overlay.append(dialog_controls["file_picker"])  # required for FilePicker to work

    page.update()
    dialog_controls["file_picker"].pick_files(
        allow_multiple=False,
        allowed_extensions=["csv", "xlsx", "xls", "txt"],
    )

async def chunk_csv_handler(e: ft.ControlEvent):


    try:
        if not save_filepath:
            await write_output("[Error] No file loaded to chunk.", e.page)
            return

        dataset_name = Path(save_filepath).stem
        paths = create_dataset_environment(dataset_name)
        chunks_dir = str(paths["chunks"])

        await write_output(f"[GUI] Chunking started: {save_filepath}", e.page)

        split_into_chunks(save_filepath, chunks_dir, chunk_size_mb=256)

        await write_output(f"[GUI] ✅ Chunking complete. Files saved in:\n{chunks_dir}", e.page)

    except Exception as ex:
        await write_output(f"[Error] Failed to chunk file: {ex}", e.page)

async def handle_chunk_button(e: ft.ControlEvent):
    """Handle the click event for the CSV chunking button.

    This routine validates the input, spawns the CSV splitting process on
    a background thread and updates the UI with progress information.
    """
    global app_busy
    page = e.page

    file_path = data_handler.saved_filepath  # ✅ get latest saved path

    if not file_path or not isinstance(file_path, (str, Path)):
        dialog_controls["chunk_status"].value = "Please load a file first."
        page.update()
        return

    dataset_name = Path(file_path).stem

    try:
        chunk_size = int(dialog_controls["chunk_size_input"].value)
    except ValueError:
        dialog_controls["chunk_status"].value = "Please enter a valid number for chunk size."
        page.update()
        return

    dialog_controls["chunk_status"].value = "Chunking in progress..."
    app_busy = True
    page.update()

    result = await asyncio.to_thread(
        split_into_chunks,
        dataset_name,
        file_path,
        chunk_size_mb=chunk_size,
        logger_fn=lambda msg: print(msg)
    )

    if result and result["total_chunks"] > 0:
        dialog_controls["chunk_status"].value = (
            f"Chunked {result['total_rows']} rows into {result['total_chunks']} files."
        )
    else:
        dialog_controls["chunk_status"].value = "Chunking failed. See logs."

    page.update()


    result = await asyncio.to_thread(
        split_into_chunks,
        dataset_name,
        save_filepath,
        chunk_size_mb=chunk_size,
        logger_fn=lambda msg: print(msg)
    )

    if result and result["total_chunks"] > 0:
        dialog_controls["chunk_status"].value = (
            f"Chunked {result['total_rows']} rows into {result['total_chunks']} files."
        )
    else:
        dialog_controls["chunk_status"].value = "Chunking failed. See logs."

    app_busy = False
    page.update()


async def on_chunk_csv(e: ft.ControlEvent):
    """Launch CSV chunking using the size stored on the page object."""
    page = e.page
    chunk_size = getattr(page, "chunk_size", CHUNK_SIZE_DEFAULT)
    dialog_controls["chunk_size_input"].value = str(chunk_size)
    print(f"[GUI] Chunk CSV requested with {chunk_size} MB")
    logging.info("Chunk CSV requested with %s MB", chunk_size)
    await handle_chunk_button(e)


def convert_file_result(e: ft.FilePickerResultEvent):
    """Store the file selected for conversion and update the display."""
    global convert_input_path
    if e.files:
        convert_input_path = e.files[0].path
        dialog_controls["convert_file_display"].value = convert_input_path
    else:
        convert_input_path = None
        dialog_controls["convert_file_display"].value = "No file selected"
    e.page.update()


def convert_dir_result(e: ft.FilePickerResultEvent):
    """Store the output directory chosen for conversion."""
    global convert_output_dir
    if e.path:
        convert_output_dir = e.path
        dialog_controls["convert_dir_display"].value = convert_output_dir
    else:
        convert_output_dir = None
        dialog_controls["convert_dir_display"].value = "No folder selected"
    e.page.update()


async def on_convert_file(e: ft.ControlEvent):
    """Convert the selected file to CSV and save it to the output directory."""
    global app_busy
    page = e.page

    if not convert_input_path or not convert_output_dir:
        dialog_controls["convert_status"].value = "Select input file and folder."
        page.update()
        return

    dialog_controls["convert_status"].value = "Converting..."
    app_busy = True
    page.update()

    try:
        output_file = await asyncio.to_thread(
            convert_file_to_csv,
            convert_input_path,
            convert_output_dir,
        )
        dialog_controls["convert_status"].value = f"Saved to {output_file}"
    except Exception as ex:
        dialog_controls["convert_status"].value = f"Error: {ex}"

    app_busy = False
    page.update()




#FILE HANDLER BLOCK END----------------------------------------------------------------------------------------
#SEAN FEATURE BUILDOUT BLOCK-----------------------------------------------------------------

async def analysis_handler(e: ft.ControlEvent):
    global app_busy
    page = e.page

    if not data_loaded:
        await write_output("[Error] Load data first.", page)
        return

    # Pull the widgets out of dialog_controls
    ad = dialog_controls["analysis_dropdown"]
    cd = dialog_controls["column_dropdown"]
    ri = dialog_controls["rows_input"]
    ss = dialog_controls["sort_switch"]

    # Read their values
    atype = ad.value
    col   = cd.value if cd.value != "All Columns" else None

    try:
        num = int(ri.value)
    except ValueError:
        await write_output("[Error] Rows must be an integer.", page)
        return

    desc = ss.value

    # Run the analysis on a background thread
    app_busy = True
    result = await asyncio.to_thread(
        run_analysis, current_df, atype, col, num, desc
    )
    await write_output(result, page)
    app_busy = False
    focus_console_tab(page)



#SEAN FEATURE BUILDOUT BLOCK END------------------------------------------------------------


# Synchronous theme toggle handler to avoid threading issues
def on_theme_toggle(e: ft.ControlEvent):
    page = e.page
    dark_mode = e.control.value
    page.theme_mode = ft.ThemeMode.DARK if dark_mode else ft.ThemeMode.LIGHT
    global current_theme_mode
    current_theme_mode = page.theme_mode

    save_theme_preference(dark_mode)  
    page.update()


async def main(page: ft.Page):
    """Primary entry point for the Flet UI."""
    global data_loaded

    # Window appearance and behavior
    page.window.frameless = True
    page.window.title_bar_hidden = True
    page.window.resizable = True
    page.vertical_alignment = ft.CrossAxisAlignment.START

    # Window size
    page.window.width = 700
    page.window.height = 600

    # Center the window on screen
    page.window.center()

    # Set app metadata
    page.title = "Protexxa Datascope - Alpha"
    # Load saved theme preference
    dark_mode_enabled = load_theme_preference()
    page.theme_mode = ft.ThemeMode.DARK if dark_mode_enabled else ft.ThemeMode.LIGHT

    page.window.icon = "assets/favicon.ico"

    # Default chunk size stored on page for reuse in handlers
    page.chunk_size = CHUNK_SIZE_DEFAULT

    page.update()

    # Start background task to flash the logo when busy
    asyncio.create_task(flash_logo(page))




    # FilePicker
    dialog_controls["file_picker"] = ft.FilePicker(
        on_result=load_data_result
    )
    page.overlay.append(dialog_controls["file_picker"])

    dialog_controls["convert_file_picker"] = ft.FilePicker(
        on_result=convert_file_result
    )
    page.overlay.append(dialog_controls["convert_file_picker"])

    dialog_controls["convert_dir_picker"] = ft.FilePicker(
        on_result=convert_dir_result
    )
    page.overlay.append(dialog_controls["convert_dir_picker"])

    # Theme toggle switch
    dialog_controls["theme_switch"] = ft.Switch(
    label="Dark Mode",
    value=dark_mode_enabled,
    on_change=on_theme_toggle
)


    # Frameless Splash screen
    splash_content = ft.Column(
        [
            ft.Text("PROPERTY OF", font_family="Helvetica", size=10, weight=ft.FontWeight.BOLD, color=ft.Colors.WHITE),
            ft.Image(src="assets/protexxa-logo.png", width=156, height=61, fit=ft.ImageFit.CONTAIN,
                     error_content=ft.Text("Logo not found", color=ft.Colors.RED)),
            ft.Text(
                "13.1°N 59.32°W → 43° 39' 11.6136'' N 79° 22' 59.4624'' W\n"
                "AICohort01: The Intelligence Migration\n"
                "Data Cleaning Division",
                font_family="Helvetica", size=10, color=ft.Colors.WHITE, text_align=ft.TextAlign.CENTER
            ),
        ],
        alignment=ft.MainAxisAlignment.CENTER,              # <--- vertical centering
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,  # <--- horizontal centering
        expand=True,
    )
    splash_container = ft.Container(
        content=splash_content,
        expand=True,
        bgcolor="#1e1e2f",
        alignment=ft.alignment.center,
        on_click=transition_to_gui,
        animate_opacity=ft.Animation(300, ft.AnimationCurve.EASE_IN_OUT),
        opacity=1.0,
    )
    dialog_controls["splash_container"] = splash_container

    page.add(splash_container)
    page.update()
    await asyncio.sleep(1)  # SPLASH SCREEN DELAY (CURRENTLY 1 SECOND FOR TESTING)
    await transition_to_gui(page)

async def transition_to_gui(page: ft.Page):
    # 1) Fade out splash screen
    splash = dialog_controls.get("splash_container")
    if splash:
        splash.opacity = 0
        page.update()
        await asyncio.sleep(0.3)

    # 2) Restore window chrome & clear splash
    page.window.frameless = False
    page.window.title_bar_hidden = False
    page.controls.clear()
    page.update()

    # 2) Header
    logo_ref = ft.Ref[ft.Image]()
    logo_img = ft.Image(
        src="assets/protexxa_logo_cropped.png",
        width=40,
        height=40,
        fit=ft.ImageFit.CONTAIN,
        error_content=ft.Text("Logo missing", color=ft.Colors.RED),
        ref=logo_ref,
    )
    header = ft.WindowDragArea(
        content=ft.Row(
            controls=[
                ft.Text(
                    "Protexxa Datascope - 1.2",
                    font_family="Arial",
                    size=20,
                    weight=ft.FontWeight.NORMAL,
                ),
                logo_img,
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            width=800,
        )
    )

    dialog_controls["logo_image"] = logo_ref

    # 3) Console & File‑ops UI (must come before Tabs)
    dialog_controls["output_text_field"] = ft.TextField(
        multiline=True, read_only=True, min_lines=20, max_lines=20,
        width=700, height=300, border_radius=20,
        border_color=ft.Colors.BLUE_GREY_200, content_padding=10, value=""
    )

    # load / test buttons
    btn_load = ft.ElevatedButton(
        text="Load Data",
        on_click=load_data_handler,
        style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=15))
    )
    dialog_controls["btn_log"] = ft.ElevatedButton(
        text="Test Logging",
        on_click=logging_handler_test,
        disabled=True, style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=15))
    )
    dialog_controls["btn_data"] = ft.ElevatedButton(
        text="Test Data Handling",
        on_click=data_handler_test,
        disabled=True, style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=15))
    )
    dialog_controls["btn_visual"] = ft.ElevatedButton(
        text="Test Visual Analyst",
        on_click=visual_analyst_test,
        disabled=True, style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=15))
    )
    button_row = ft.Row(
        controls=[
            btn_load,
            dialog_controls["btn_log"],
            dialog_controls["btn_data"],
            dialog_controls["btn_visual"]
        ],
        alignment=ft.MainAxisAlignment.CENTER,
        spacing=10
    )

    file_ops_frame = ft.Container(
        content=ft.Column([
            ft.Text("(File save buttons will go here)", color=ft.Colors.GREY_600)
        ]),
        border_radius=10,
        border=ft.border.all(1, ft.Colors.BLUE_GREY_200),
        padding=10,
        width=700
    )

    dialog_controls["status_label"] = ft.Text("Ready", color=ft.Colors.BLUE)

    # 4) Advanced Tab Widgets (SEAN FEATURE BUILDOUT)
    analysis_dropdown = ft.Dropdown(
        label="Analysis Type",
        width=200,
        options=[
            ft.dropdown.Option("Data Preview"),
            ft.dropdown.Option("Missing Values"),
            ft.dropdown.Option("Duplicate Detection"),
            ft.dropdown.Option("Placeholder Detection"),
            ft.dropdown.Option("Special Character Analysis"),
        ],
        on_change=lambda e: focus_console_tab(e.page)
    )
    column_dropdown = ft.Dropdown(
        label="Column",
        width=200,
        options=[ft.dropdown.Option("All Columns")]
    )
    rows_input = ft.TextField(label="Rows to show", value="10", width=100)
    sort_switch = ft.Switch(label="Descending order", value=False)
    run_btn = ft.ElevatedButton("Run Analysis", on_click=analysis_handler)

    # stash for the handler
    dialog_controls["analysis_dropdown"] = analysis_dropdown
    dialog_controls["column_dropdown"]   = column_dropdown
    dialog_controls["rows_input"]        = rows_input
    dialog_controls["sort_switch"]       = sort_switch

    advanced_content = ft.Column([
        analysis_dropdown,
        column_dropdown,
        ft.Row([rows_input, sort_switch], spacing=20),
        run_btn
    ], spacing=10, alignment=ft.MainAxisAlignment.START)

    # 5) Build Tabs in one shot
    dialog_controls["chunk_size_input"] = ft.TextField(
        label="Chunk size (MB)",
        width=200,
        value=str(page.chunk_size),
        on_change=lambda e: setattr(page, "chunk_size", int(e.control.value))
    )
    dialog_controls["chunk_status"] = ft.Text(value="", color=ft.Colors.GREY_700)

    csv_chunker_card = ft.Card(
        elevation=3,
        content=ft.Container(
            padding=16,
            content=ft.Column(
                [
                    ft.Text("CSV Chunker", style="headlineSmall"),
                    ft.Row(
                        [
                            dialog_controls["chunk_size_input"],
                            ft.ElevatedButton(
                                text="Chunk CSV",
                                icon=SPLIT_CSV_ICON,
                                on_click=on_chunk_csv,
                            ),
                        ],
                        spacing=16,
                        alignment="start",
                    ),
                ]
            ),
        ),
    )

    dialog_controls["convert_status"] = ft.Text(value="", color=ft.Colors.GREY_700)
    dialog_controls["convert_file_display"] = ft.Text("No file selected", color=ft.Colors.GREY_700)
    dialog_controls["convert_dir_display"] = ft.Text("No folder selected", color=ft.Colors.GREY_700)

    convert_card = ft.Card(
        elevation=3,
        content=ft.Container(
            padding=16,
            content=ft.Column(
                [
                    ft.Text("File Converter", style="headlineSmall"),
                    ft.Row(
                        [
                            ft.ElevatedButton(
                                text="Select File",
                                icon=ft.icons.UPLOAD_FILE,
                                on_click=lambda e: dialog_controls["convert_file_picker"].pick_files(allow_multiple=False),
                            ),
                            dialog_controls["convert_file_display"],
                        ],
                        spacing=16,
                        alignment="start",
                    ),
                    ft.Row(
                        [
                            ft.ElevatedButton(
                                text="Choose Folder",
                                icon=ft.icons.FOLDER_OPEN,
                                on_click=lambda e: dialog_controls["convert_dir_picker"].get_directory_path(),
                            ),
                            dialog_controls["convert_dir_display"],
                        ],
                        spacing=16,
                        alignment="start",
                    ),
                    ft.ElevatedButton(
                        text="Convert to CSV",
                        icon=ft.icons.DOWNLOAD,
                        on_click=on_convert_file,
                    ),
                    dialog_controls["convert_status"],
                ]
            ),
        ),
    )

    tabs = ft.Tabs(
        selected_index=0,
        animation_duration=200,
        expand=True,
        tabs=[
            ft.Tab(
                text="Console",
                content=ft.Column([
                    dialog_controls["output_text_field"],
                    button_row,
                    file_ops_frame,
                    dialog_controls["status_label"],
                ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=10)
            ),
            ft.Tab(
                text="Data Tools",
                content=ft.Column([
                    ft.Text("🔧 Data Tools", style="titleMedium", color=ft.Colors.GREY_800),
                    csv_chunker_card,
                    dialog_controls["chunk_status"],
                    convert_card,
                ])
            ),
            ft.Tab(
                text="Advanced tools",
                content=advanced_content
            ),
            ft.Tab(
                text="Settings",
                content=ft.Column([
                    ft.Text("⚙️ Settings and preferences", color=ft.Colors.GREY_500),
                    dialog_controls["theme_switch"]
                ])
            )
        ]
    )
    dialog_controls["tabs"] = tabs


    # 6) Render with fade-in
    main_container = ft.Column([
        header,
        tabs
    ], expand=True, opacity=0.0, animate_opacity=ft.Animation(300, ft.AnimationCurve.EASE_IN_OUT))
    page.add(main_container)
    page.update()
    main_container.opacity = 1.0
    page.update()

    
if __name__ == "__main__":
    ft.app(target=main, assets_dir="assets")
