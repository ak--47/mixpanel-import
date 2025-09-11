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

# Expose port 8080 (required by Cloud Run)
EXPOSE 8080

# Start the application
CMD ["npm", "start"]