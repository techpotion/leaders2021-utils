from scripts import migrate_dataset, circles, parks, pollution, regions_density

def main():
    migrate_dataset.main()
    circles.main()
    parks.main()
    pollution.main()
    regions_density.main()

if __name__ == "__main__":
    main()