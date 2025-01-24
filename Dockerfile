# Check out https://hub.docker.com/_/node to select a new base image
FROM python:3.9-buster

# Install node
ENV NODE_VERSION=20.9.0
RUN apt install -y curl
RUN curl -o- https://raw.githubusercontent.com/creationix/nvm/v0.34.0/install.sh | bash
ENV NVM_DIR=/root/.nvm
RUN . "$NVM_DIR/nvm.sh" && nvm install ${NODE_VERSION}
RUN . "$NVM_DIR/nvm.sh" && nvm use v${NODE_VERSION}
RUN . "$NVM_DIR/nvm.sh" && nvm alias default v${NODE_VERSION}
ENV PATH="/root/.nvm/versions/node/v${NODE_VERSION}/bin/:${PATH}"
RUN node --version
RUN npm --version

RUN mkdir -p /usr/src/app
# Change directory so that our commands run inside this new directory
WORKDIR /usr/src/app
# Copy dependency definitions
COPY package*.json /usr/src/app
# Get all the code needed to run the app
COPY . /usr/src/app

# Add startup script and make it executable
COPY start.sh /usr/src/app/
RUN chmod +x /usr/src/app/start.sh

RUN npm install -g yarn
RUN yarn global add pm2
RUN yarn install

RUN pip3 install --timeout 10000 --no-cache-dir --upgrade pip && \
    pip3 install --timeout 10000 --no-cache-dir --index-url=https://www.piwheels.org/simple --extra-index-url=https://pypi.python.org/simple/ -r python_models/requirements.txt

# Add huggingface-hub to requirements
RUN pip3 install --no-cache-dir huggingface-hub

# Bind to all network interfaces so that it can be mapped to the host OS
ENV HOST=0.0.0.0 PORT=3000

EXPOSE ${PORT}
CMD ["/usr/src/app/start.sh"]
