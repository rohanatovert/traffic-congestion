import gym
from gym import spaces
import numpy as np

class TrafficEnv(gym.Env):
    """
    A simple traffic simulation environment for OpenAI Gym.
    Cars are represented by integers on a one-dimensional road.
    """
    
    def __init__(self):
        super(TrafficEnv, self).__init__()
        self.length = 100  # Length of the road
        self.positions = np.zeros(self.length)  # Positions of cars on the road
        self.num_cars = 10  # Number of cars on the road
        self.action_space = spaces.Discrete(2)  # Accelerate or decelerate
        self.observation_space = spaces.Box(low=0, high=1, shape=(self.length,), dtype=np.float32)

    def reset(self):
        self.positions = np.zeros(self.length)
        car_positions = np.random.choice(range(self.length), self.num_cars, replace=False)
        self.positions[car_positions] = 1  # Place cars at random positions
        return self.positions

    def step(self, action):
        reward = 0
        done = False
        info = {}

        # Example logic for moving cars (simplified)
        if action == 0:  # Decelerate
            reward = -1
        elif action == 1:  # Accelerate
            reward = 1
            # Move cars forward (this is a very simplified logic)
            new_positions = np.roll(self.positions, 1)
            self.positions = new_positions
        
        # Example condition to end the episode
        if np.random.rand() < 0.05:  # Randomly end the episode
            done = True
        
        return self.positions, reward, done, info

    def render(self, mode='human'):
        print(''.join(['1' if x == 1 else '.' for x in self.positions]))

# Example usage
env = TrafficEnv()
state = env.reset()
done = False

while not done:
    action = env.action_space.sample()  # Choose a random action
    state, reward, done, info = env.step(action)
    env.render()
