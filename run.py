from src import main

if __name__ == "__main__":
    # Get system arguments
    import sys
    justStats = False
    if len(sys.argv) > 1:
        if sys.argv[1] == 'stats':
            justStats = True
    main(justStats)



