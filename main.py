
def main():
    print("Fetching Todoist data...")
    from connectors import todoist
    todoist.main()

    # print("Fetching Strava data...")
    # from connectors import strava
    # strava.main()


if __name__ == "__main__":
    main()
