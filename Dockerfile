# Use official Node.js runtime as base image
FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy package files for better Docker layer caching
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy application code
COPY . .

# Set environment variables for Cloud Run
ENV NODE_ENV=production
ENV PORT=8080

# Set Node.js options for serverless environment
# --max-old-space-size: Set heap size to match Cloud Run memory (8GB instance)
# --expose-gc: Allow manual garbage collection for memory-critical operations
# With 8GB Cloud Run: allocate 7GB to heap (leaving 1GB for native memory, buffers, OS)
ENV NODE_OPTIONS="--max-old-space-size=7168 --expose-gc"

# Expose port 8080 (required by Cloud Run)
EXPOSE 8080

# Start the application
CMD ["npm", "start"]