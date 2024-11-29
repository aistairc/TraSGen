package IDMtest;


public class IDMExample {

    // IDM Parameters (same as before)
    static double desiredVelocity = 80.0; //  (m/s)
    static double maxAcceleration = 1.5;  //  (m/s^2)      //a
    static double desiredTimeGap = 1.8;   // (s)           //T
    static double minSpacing = 5.0;       // (m)
    static double comfortableDeceleration = 1.0; //(m/s^2) //b

    private static final double minAcceleration = -9.8; // Maximum deceleration (emergency braking, m/s^2)



    // IDM acceleration calculation
    public static double calculateAcceleration(double velocity, double distanceToLead, double leadVelocity) {
        double freeRoadTerm = 1 - Math.pow(velocity / desiredVelocity, 4);
        double deltaV = velocity - leadVelocity;
        double safeDistance = minSpacing + Math.max(0, velocity * desiredTimeGap + (velocity * deltaV) / (2 * Math.sqrt(maxAcceleration * comfortableDeceleration)));
        double interactionTerm = Math.pow(safeDistance / distanceToLead, 2);
        double rawAcceleration = maxAcceleration * (freeRoadTerm - interactionTerm);

        // Clamp acceleration to minimum and maximum allowed values
        return Math.max(minAcceleration, rawAcceleration);
    }

    public static void main(String[] args) {
        // Initial conditions
        double followVelocity = 1.5; // Current velocity (m/s)
        double leadVelocity = 1.5;  // Velocity of the leading vehicle (m/s)
        double distanceToLead = 10.0; // Distance to the leading vehicle (m)

        // Timestep
        double timeStep = 1; // seconds

        // Calculate acceleration
        double acceleration = calculateAcceleration(followVelocity, distanceToLead, leadVelocity);

        // Update velocity using a single timestep
        double newVelocity = followVelocity + acceleration * timeStep;

        // Print results
        System.out.printf("Current Velocity: %.2f m/s%n", followVelocity);
        System.out.printf("Acceleration: %.2f m/s²%n", acceleration);
        System.out.printf("New Velocity after %.1f seconds: %.2f m/s%n", timeStep, newVelocity);
    }
}
