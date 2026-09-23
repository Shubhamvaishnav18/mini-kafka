# =========================
# Stage 1: Build
# =========================
FROM maven:3.9-eclipse-temurin-26 AS build

WORKDIR /app

# Copy Maven configuration first
COPY pom.xml .

# Download dependencies
RUN mvn dependency:go-offline

# Copy source code
COPY src ./src

# Build application
RUN mvn clean package -DskipTests


# =========================
# Stage 2: Runtime
# =========================
FROM eclipse-temurin:26-jre

WORKDIR /app

# Create non-root user
RUN useradd -r -u 1001 appuser

# Create application data directory
RUN mkdir -p /app/data

# Copy generated JAR
COPY --from=build --chown=appuser:appuser \
     /app/target/*.jar app.jar

# Give appuser ownership of the application directory
RUN chown -R appuser:appuser /app

# Application port
EXPOSE 8080

# Run as non-root user
USER appuser

# Start application
ENTRYPOINT ["java", "-jar", "app.jar"]