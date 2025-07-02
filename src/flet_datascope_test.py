import flet as ft
import asyncio
import os
from data_handler import create_dataset_environment, load_data
from pathlib import Path


# Global flag for checking if dataset was loaded
data_loaded = False

# Control references
dialog_controls = {
    "output_text_field": None,
    "btn_log": None,
    "btn_data": None,
    "btn_visual": None,
    "status_label": None,
    "file_picker": None,
    "theme_switch": None,
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
from data_handler import save_filepath, get_data_stats
#-----------------------------------------------------------------------

async def load_data_result(e: ft.FilePickerResultEvent):
    page = e.page
    global data_loaded

    if e.files:
        file_path = e.files[0].path
        await write_output(f"[Load Data] File selected: {file_path}", page)

        # 1. Get dataset name from file
        dataset_name = Path(file_path).stem

        # 2. Create environment folders for this dataset
        project_paths = create_dataset_environment(dataset_name)
        await write_output(f"[Environment] Folders created at: {project_paths['project']}", page)

        # 3. Load the dataset
        df = load_data(file_path)
        if df is None:
            await write_output("[Error] Failed to load dataset. Please check the file format.", page)
            return

        info = get_data_stats(df, file_path)
        await write_output(info["log1"], page)
        await write_output(info["log2"], page)

        # 4. Toggle buttons now that loading succeeded
        data_loaded = True
        dialog_controls["btn_log"].disabled = False
        dialog_controls["btn_data"].disabled = False
        dialog_controls["btn_visual"].disabled = False
        dialog_controls["status_label"].value = "Ready"
        dialog_controls["status_label"].color = ft.Colors.BLUE

    else:
        await write_output("[Load Data] No file selected.", page)
        dialog_controls["status_label"].value = "Ready"
        dialog_controls["status_label"].color = ft.Colors.BLUE

    page.update()


async def load_data_handler(e: ft.ControlEvent):
    page = e.page
    dialog_controls["status_label"].value = "Processing..."
    dialog_controls["status_label"].color = ft.Colors.ORANGE
    dialog_controls["file_picker"].pick_files(allow_multiple=False, allowed_extensions=["csv", "xlsx", "xls"])
    page.update()
    

#FILE HANDLER BLOCK END----------------------------------------------------------------------------------------



# Synchronous theme toggle handler to avoid threading issues
def on_theme_toggle(e: ft.ControlEvent):
    page = e.page
    # Toggle between light and dark themes
    page.theme_mode = ft.ThemeMode.DARK if e.control.value else ft.ThemeMode.LIGHT
    page.update()

async def main(page: ft.Page):
    global data_loaded
    # Enable frameless splash window
    page.window.frameless = True
    page.window.title_bar_hidden = True
    page.update()

    # Set default theme
    page.theme_mode = ft.ThemeMode.LIGHT
    page.title = "Protexxa Datascope - Alpha"
    page.window_width = 800
    page.window_height = 600
    page.window_resizable = True
    page.vertical_alignment = ft.CrossAxisAlignment.START
    page.window.icon = "./dataScope/assets/DataScopeLogo.ico"

    # FilePicker
    dialog_controls["file_picker"] = ft.FilePicker(
        on_result=load_data_result
    )
    page.overlay.append(dialog_controls["file_picker"])

    # Theme toggle switch
    dialog_controls["theme_switch"] = ft.Switch(
        label="Dark Mode",
        value=False,
        on_change=on_theme_toggle
    )

    # Frameless Splash screen
    splash_content = ft.Column(
        [
            ft.Text("PROPERTY OF", font_family="Helvetica", size=10, weight=ft.FontWeight.BOLD, color=ft.Colors.WHITE),
            ft.Image(src="dataScope/assets/protexxa-logo.png", width=156, height=61, fit=ft.ImageFit.CONTAIN,
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
    )

    page.add(splash_container)
    page.update()
    await asyncio.sleep(1)  # SPLASH SCREEN DELAY (CURRENTLY 1 SECOND FOR TESTING)
    await transition_to_gui(page)

async def transition_to_gui(page: ft.Page):
    # Restore window chrome
    page.window.frameless = False
    page.window.title_bar_hidden = False
    page.update()

    page.controls.clear()
    # Header with draggable area and theme switch
    header = ft.WindowDragArea(
        content=ft.Row(
            controls=[
                ft.Text("Protexxa Datascope - Alpha 1.0", font_family="Arial", size=20, weight=ft.FontWeight.NORMAL),
                dialog_controls["theme_switch"],
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            width=800,
        )
    )

    # Output console
    dialog_controls["output_text_field"] = ft.TextField(
        multiline=True, read_only=True,
        min_lines=20, max_lines=20,
        width=700, height=300,
        border_radius=ft.border_radius.all(20),
        border_color=ft.Colors.BLUE_GREY_200,
        content_padding=10,
        value="",
    )

    # Buttons
    btn_load = ft.ElevatedButton(text="Load Data",
                                 on_click=load_data_handler,
                                 style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=15)))
    dialog_controls.update({
        "btn_log": ft.ElevatedButton(text="Test Logging & Error Handler",
                                       on_click=logging_handler_test,
                                       disabled=True, style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=15))),
        "btn_data": ft.ElevatedButton(text="Test Data Handling",
                                        on_click=data_handler_test,
                                        disabled=True, style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=15))),
        "btn_visual": ft.ElevatedButton(text="Test Visual Analyst",
                                          on_click=visual_analyst_test,
                                          disabled=True, style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=15)))
    })
    button_row = ft.Row(controls=[btn_load, dialog_controls["btn_log"], dialog_controls["btn_data"], dialog_controls["btn_visual"]],
                        alignment=ft.MainAxisAlignment.CENTER, spacing=10)

    # File ops placeholder
    file_ops_frame = ft.Container(content=ft.Column([
        ft.Text("(File save buttons and options will be updated post Day-0)", color=ft.Colors.GREY_600)
    ]), border_radius=ft.border_radius.all(10), border=ft.border.all(1, ft.Colors.BLUE_GREY_200),
        padding=10, margin=ft.margin.only(top=0, left=0, right=0, bottom=0), alignment=ft.alignment.center, width=700)

    # Status bar
    dialog_controls["status_label"] = ft.Text("Ready", color=ft.Colors.BLUE)

    # Assemble GUI
    page.add(
        ft.Column([
            header,
            dialog_controls["output_text_field"],
            button_row,
            file_ops_frame,
            dialog_controls["status_label"],
        ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=10, expand=True)
    )
    page.update()

if __name__ == "__main__":
    ft.app(target=main, assets_dir="assets")
