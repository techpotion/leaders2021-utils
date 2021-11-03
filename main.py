from scripts import migrate_dataset, circles, parks, pollution, regions_density, subways

def main():
    migrate_dataset.main()
    circles.main()
    parks.main()
    pollution.main()
    regions_density.main()
    subways.main()

if __name__ == "__main__":
    main()