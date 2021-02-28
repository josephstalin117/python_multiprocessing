import torch
import torch.nn as nn
import torch.nn.functional as F

class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.fc = nn.Linear(1, 3)
    
    def forward(self, x):
        x = self.fc(x)
        x = F.relu(x)
        return x