# main.py
# This script listens for new messages from a Google Cloud Pub/Sub subscription,
# extracts a deal ID, and simulates a "translation" task.

import os
import json
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

# --- Configuration ---
# Replace with your actual project and subscription IDs.
PROJECT_ID = "rich-boulevard-461222-d8"
SUBSCRIPTION_ID = "pipedrive-webhooks-sub"

# The timeout in seconds for the pull request.
TIMEOUT_SECONDS = 5.0


# --- Functions ---

def translate_deal_data(deal_id: str):
    """
    This function simulates the "translation" or processing of the deal ID.

    In a real-world scenario, you would replace this with your actual business logic.
    For example:
    1. Make an API call to a CRM using the deal_id to fetch deal details.
    2. Transform the fetched data into a new format.
    3. Push the transformed data to another system or database.

    Args:
        deal_id (str): The ID of the deal to be processed.
    """
    print(f"[{os.getpid()}] Translating deal ID: {deal_id}")

    # Add your specific business logic here.
    # For example:
    # try:
    #     deal_details = fetch_deal_from_crm(deal_id)
    #     translated_data = transform_data(deal_details)
    #     push_to_new_system(translated_data)
    #     print(f"Successfully processed deal: {deal_id}")
    # except Exception as e:
    #     print(f"Failed to process deal {deal_id}: {e}")
    #     # You may want to log this to a dead-letter topic or a logging service.


# --- Main Program ---

def main():
    """
    The main function that sets up the Pub/Sub listener.
    """
    # Create a subscriber client. This client is used to communicate with the Pub/Sub service.
    subscriber = pubsub_v1.SubscriberClient()

    # Create the fully qualified path for the subscription.
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    def callback(message: pubsub_v1.subscriber.message.Message):
        """
        Callback function for when a new message is received.
        """
        try:
            # The message data is a byte string. Decode it to a string.
            deal_id = message.data.decode("utf-8")
            print(f"Received deal ID: {deal_id}. Starting translation...")

            # Call the "translation" function to process the deal ID.
            translate_deal_data(deal_id)

            # Acknowledge the message. This tells Pub/Sub that the message has been
            # successfully processed and should not be redelivered.
            message.ack()
            print(f"Acknowledged message for deal ID: {deal_id}")

        except Exception as e:
            # If any error occurs during processing, log it and nack (negatively
            # acknowledge) the message so it can be redelivered.
            print(f"Error processing message: {e}")
            message.nack()

    # Start the subscriber loop.
    print(f"Listening for messages on {subscription_path}...")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    # Use a try-except block to gracefully handle Ctrl+C to stop the listener.
    with subscriber:
        try:
            # This is a blocking call that will wait for messages.
            # You can also add a timeout to handle scenarios with no messages.
            streaming_pull_future.result()
        except TimeoutError:
            print("No messages received within the timeout period.")
            streaming_pull_future.cancel()
            streaming_pull_future.result()  # Wait for the pull to cancel.
        except KeyboardInterrupt:
            # Gracefully close the listener on a keyboard interrupt (Ctrl+C).
            streaming_pull_future.cancel()
            print("Stopped listening. Shutting down...")


if __name__ == "__main__":
    main()