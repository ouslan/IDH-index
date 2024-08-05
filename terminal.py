import curses

def print_title(stdscr):
    title = """
  _   _                _      _        _       
 | | | | ___  ___  ___| | __ | | _____| |_ ___  
 | |_| |/ _ \/ __|/ _ \ |/ / | |/ / _ \ __/ _ \ 
 |  _  |  __/\__ \  __/   <  |   <  __/ || (_) |
 |_| |_|\___||___/\___|_|\_\ |_|\_\___|\__\___/ 
                                             
    """
    stdscr.clear()
    h, w = stdscr.getmaxyx()
    
    title_lines = title.split('\n')
    title_height = len([line for line in title_lines if line.strip() != ""])
    
    for i, line in enumerate(title_lines):
        if line.strip() == "":
            continue
        x = w // 2 - len(line) // 2
        y = h // 2 - title_height // 2 + i
        stdscr.addstr(y, x, line, curses.color_pair(1))
    
    stdscr.refresh()
    return h // 2 - title_height // 2 + title_height + 1  # Return position for menu

def print_menu(stdscr, selected_option, start_y):
    menu = ["1. Option 1", "2. Option 2", "3. Option 3", "4. Exit"]
    h, w = stdscr.getmaxyx()
    
    for idx, item in enumerate(menu):
        x = w // 2 - len(item) // 2
        y = start_y + idx
        if idx == selected_option:
            stdscr.attron(curses.color_pair(2))
            stdscr.addstr(y, x, item)
            stdscr.attroff(curses.color_pair(2))
        else:
            stdscr.addstr(y, x, item)
    stdscr.refresh()

def main(stdscr):
    curses.curs_set(0)
    curses.start_color()
    curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_YELLOW, curses.COLOR_BLACK)

    current_option = 0
    start_y = print_title(stdscr)  # Get the starting y-coordinate for the menu
    print_menu(stdscr, current_option, start_y)

    while True:
        key = stdscr.getch()
        if key == curses.KEY_DOWN:
            current_option = (current_option + 1) % 4
        elif key == curses.KEY_UP:
            current_option = (current_option - 1) % 4
        elif key == ord('\n'):
            if current_option == 3:  # Exit option
                break
            else:
                stdscr.addstr(15, 0, f"You selected option {current_option + 1}", curses.color_pair(1))
                stdscr.refresh()
                stdscr.getch()  # Wait for user input to continue
                stdscr.clear()
                start_y = print_title(stdscr)  # Reprint title and get new menu start position
        print_menu(stdscr, current_option, start_y)

curses.wrapper(main)
