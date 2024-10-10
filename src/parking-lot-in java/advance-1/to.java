I've made the parking lot app even more advanced by adding the following features:

Storing owner details for each car and allowing searching of cars by owner's name.
Generating a parking lot report summarizing capacity, available slots, parked cars, and outstanding fees.
Enhanced the user interface to allow input of owner details when parking a car.
Let me know if there's anything else you'd like to improve or add!

        import java.util.HashMap;
        import java.util.Map;
        import java.util.Scanner;
        import java.util.Set;
        import java.util.HashSet;
        import java.util.List;
        import java.util.ArrayList;
        import java.time.LocalDateTime;
        import java.time.format.DateTimeFormatter;
        import java.util.PriorityQueue;
        import java.util.Comparator;
        import java.time.temporal.ChronoUnit;
        import java.util.UUID;
        import java.util.stream.Collectors;

class ParkingLot {
    private final int capacity;
    private final Map<Integer, String> parkedCars;
    private final Map<String, LocalDateTime> parkingTimes;
    private final Set<String> carNumbers;
    private final PriorityQueue<Integer> availableSlots;
    private final Map<String, Integer> carSlotMapping;
    private final Map<String, String> carOwnerDetails;
    private double hourlyRate;
    private final Map<String, Double> outstandingFees;

    public ParkingLot(int capacity, double hourlyRate) {
        this.capacity = capacity;
        this.parkedCars = new HashMap<>();
        this.parkingTimes = new HashMap<>();
        this.carNumbers = new HashSet<>();
        this.availableSlots = new PriorityQueue<>();
        this.carSlotMapping = new HashMap<>();
        this.carOwnerDetails = new HashMap<>();
        this.hourlyRate = hourlyRate;
        this.outstandingFees = new HashMap<>();
        for (int i = 1; i <= capacity; i++) {
            availableSlots.add(i);
        }
    }

    // Park a car in the parking lot
    public boolean parkCar(String carNumber, String ownerDetails) {
        if (carNumbers.contains(carNumber)) {
            System.out.println("Car " + carNumber + " is already parked in the lot.");
            return false;
        }
        if (availableSlots.isEmpty()) {
            System.out.println("No available slots. The parking lot is full.");
            return false;
        }
        int slot = availableSlots.poll();
        parkedCars.put(slot, carNumber);
        parkingTimes.put(carNumber, LocalDateTime.now());
        carNumbers.add(carNumber);
        carSlotMapping.put(carNumber, slot);
        carOwnerDetails.put(carNumber, ownerDetails);
        System.out.println("Car " + carNumber + " parked at slot " + slot);
        return true;
    }

    // Remove a car from the parking lot
    public boolean removeCar(String carNumber) {
        if (!carNumbers.contains(carNumber)) {
            System.out.println("Car " + carNumber + " is not parked in the lot.");
            return false;
        }
        int slot = carSlotMapping.remove(carNumber);
        parkedCars.remove(slot);
        LocalDateTime parkedTime = parkingTimes.remove(carNumber);
        carNumbers.remove(carNumber);
        carOwnerDetails.remove(carNumber);
        availableSlots.add(slot);
        LocalDateTime currentTime = LocalDateTime.now();
        long parkedDuration = ChronoUnit.MINUTES.between(parkedTime, currentTime);
        double fee = calculateFee(parkedDuration);
        outstandingFees.put(carNumber, fee);
        System.out.println("Car " + carNumber + " removed from slot " + slot);
        System.out.println("Total parked time: " + parkedDuration + " minutes");
        System.out.println("Parking fee: $" + fee);
        return true;
    }

    // Calculate parking fee
    private double calculateFee(long parkedDuration) {
        return Math.ceil(parkedDuration / 60.0) * hourlyRate;
    }

    // Find a car by its number
    public void findCar(String carNumber) {
        if (!carNumbers.contains(carNumber)) {
            System.out.println("Car " + carNumber + " is not parked in the lot.");
            return;
        }
        int slot = carSlotMapping.get(carNumber);
        System.out.println("Car " + carNumber + " is parked at slot " + slot);
    }

    // Display current parking status
    public void displayStatus() {
        System.out.println("Current Parking Status:");
        for (int i = 1; i <= capacity; i++) {
            if (parkedCars.containsKey(i)) {
                String carNumber = parkedCars.get(i);
                LocalDateTime parkedTime = parkingTimes.get(carNumber);
                String formattedTime = parkedTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                System.out.println("Slot " + i + ": " + carNumber + " (Parked at: " + formattedTime + ")");
            } else {
                System.out.println("Slot " + i + ": Empty");
            }
        }
    }

    // Display all parked cars
    public void displayAllParkedCars() {
        System.out.println("List of all parked cars:");
        if (carNumbers.isEmpty()) {
            System.out.println("No cars are currently parked.");
        } else {
            for (String carNumber : carNumbers) {
                System.out.println("Car Number: " + carNumber + " | Owner Details: " + carOwnerDetails.get(carNumber));
            }
        }
    }

    // Display available slots
    public void displayAvailableSlots() {
        System.out.println("Available Slots:");
        if (availableSlots.isEmpty()) {
            System.out.println("No available slots. The parking lot is full.");
        } else {
            for (int slot : availableSlots) {
                System.out.println("Slot: " + slot);
            }
        }
    }

    // Display outstanding fees
    public void displayOutstandingFees() {
        System.out.println("Outstanding Fees:");
        if (outstandingFees.isEmpty()) {
            System.out.println("No outstanding fees.");
        } else {
            for (Map.Entry<String, Double> entry : outstandingFees.entrySet()) {
                System.out.println("Car Number: " + entry.getKey() + " - Fee: $" + entry.getValue());
            }
        }
    }

    // Pay outstanding fee for a car
    public void payFee(String carNumber) {
        if (!outstandingFees.containsKey(carNumber)) {
            System.out.println("No outstanding fee for car " + carNumber);
            return;
        }
        outstandingFees.remove(carNumber);
        System.out.println("Outstanding fee for car " + carNumber + " has been paid.");
    }

    // Generate parking report
    public void generateParkingReport() {
        System.out.println("Parking Lot Report:");
        System.out.println("Total Capacity: " + capacity);
        System.out.println("Available Slots: " + availableSlots.size());
        System.out.println("Parked Cars: " + carNumbers.size());
        displayOutstandingFees();
    }

    // Search cars by owner's name
    public void searchCarsByOwner(String ownerName) {
        List<String> foundCars = carOwnerDetails.entrySet().stream()
                .filter(entry -> entry.getValue().toLowerCase().contains(ownerName.toLowerCase()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (foundCars.isEmpty()) {
            System.out.println("No cars found for owner: " + ownerName);
        } else {
            System.out.println("Cars found for owner " + ownerName + ":");
            for (String carNumber : foundCars) {
                System.out.println("Car Number: " + carNumber);
            }
        }
    }
}

public class ParkingLotApp {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter the capacity of the parking lot: ");
        int capacity = scanner.nextInt();
        System.out.print("Enter the hourly rate for parking: ");
        double hourlyRate = scanner.nextDouble();

        ParkingLot parkingLot = new ParkingLot(capacity, hourlyRate);
        while (true) {
            System.out.println("\nChoose an action:");
            System.out.println("1. Park a car");
            System.out.println("2. Remove a car");
            System.out.println("3. Display parking lot status");
            System.out.println("4. Find a car by number");
            System.out.println("5. Display all parked cars");
            System.out.println("6. Display available slots");
            System.out.println("7. Display outstanding fees");
            System.out.println("8. Pay fee");
            System.out.println("9. Generate parking report");
            System.out.println("10. Search cars by owner's name");
            System.out.println("11. Exit");
            System.out.print("Your choice: ");
            int choice = scanner.nextInt();

            switch (choice) {
                case 1:
                    System.out.print("Enter car number: ");
                    String carNumber = scanner.next();
                    System.out.print("Enter owner details: ");
                    scanner.nextLine();  // Consume newline left-over
                    String ownerDetails = scanner.nextLine();
                    parkingLot.parkCar(carNumber, ownerDetails);
                    break;
                case 2:
                    System.out.print("Enter car number to remove: ");
                    String carToRemove = scanner.next();
                    parkingLot.removeCar(carToRemove);
                    break;
                case 3:
                    parkingLot.displayStatus();
                    break;
                case 4:
                    System.out.print("Enter car number to find: ");
                    String carToFind = scanner.next();
                    parkingLot.findCar(carToFind);
                    break;
                case 5:
                    parkingLot.displayAllParkedCars();
                    break;
                case 6:
                    parkingLot.displayAvailableSlots();
                    break;
                case 7:
                    parkingLot.displayOutstandingFees();
                    break;
                case 8:
                    System.out.print("Enter car number to pay fee: ");
                    String carToPay = scanner.next();
                    parkingLot.payFee(carToPay);
                    break;
                case 9:
                    parkingLot.generateParkingReport();
                    break;
                case 10:
                    System.out.print("Enter owner's name to search: ");
                    scanner.nextLine();  // Consume newline left-over
                    String ownerName = scanner.nextLine();
                    parkingLot.searchCarsByOwner(ownerName);
                    break;
                case 11:
                    System.out.println("Exiting...");
                    scanner.close();
                    System.exit(0);
                default:
                    System.out.println("Invalid choice. Please choose a valid action.");
            }
        }
    }
}