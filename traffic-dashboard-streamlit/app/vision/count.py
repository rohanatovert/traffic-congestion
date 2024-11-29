import cv2
import numpy as np

# Load YOLO
net = cv2.dnn.readNet("yolov3.weights", "yolov3.cfg")
layer_names = net.getLayerNames()
output_layers = [layer_names[i[0] - 1] for i in net.getUnconnectedOutLayers()]

# Load video
cap = cv2.VideoCapture('path_to_your_video')

while True:
    _, frame = cap.read()
    height, width, channels = frame.shape

    # Detecting objects
    blob = cv2.dnn.blobFromImage(frame, 0.00392, (416, 416), (0, 0, 0), True, crop=False)
    net.setInput(blob)
    outs = net.forward(output_layers)

    # Information to plot later on the map
    class_ids = []
    confidences = []
    boxes = []

    for out in outs:
        for detection in out:
            scores = detection[5:]
            class_id = np.argmax(scores)
            confidence = scores[class_id]
            if confidence > 0.5:
                # Object detected
                center_x = int(detection[0] * width)
                center_y = int(detection[1] * height)
                w = int(detection[2] * width)
                h = int(detection[3] * height)

                # Rectangle coordinates
                x = int(center_x - w / 2)
                y = int(center_y - h / 2)

                boxes.append([x, y, w, h])
                confidences.append(float(confidence))
                class_ids.append(class_id)

    # Only proceed if at least one vehicle is detected
    if len(boxes) > 0:
        # Here you could filter by class_ids to ensure only vehicles are considered
        # For simplicity, assume every detection is a vehicle
        # Process vehicle count and coordinates (mocked as center_x, center_y)
        print(f"Vehicles detected: {len(boxes)} at positions: {[(box[0], box[1]) for box in boxes]}")
    
    # To display the frame with detected boxes, uncomment below lines
    # for i in range(len(boxes)):
    #     x, y, w, h = boxes[i]
    #     cv2.rectangle(frame, (x, y), (x + w, y + h), (0, 255, 0), 2)
    # cv2.imshow('Frame', frame)

    key = cv2.waitKey(1)
    if key == 27:  # ESC key to break
        break

cap.release()
cv2.destroyAllWindows()
