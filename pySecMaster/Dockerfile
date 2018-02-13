FROM python:3
LABEL maintainer "joshschertz3@gmail.com"

# Install all required python libraries
COPY requirements.txt ./
RUN pip3 install -r requirements.txt

# Move all pySecMaster files into the container
COPY . .

# Set the default endpoint as the pySecMaster.py file, using the system python
ENTRYPOINT ["pySecMaster.py"]
# Download daily prices from quandl, yahoo and google, then run the
#   cross-validator for only 30 prior periods
CMD ["--daily-downloads", "quandl", "yahoo", "google",
     "--validator-period", "30",
     "--verbose"]
