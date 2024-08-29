from dataclasses import dataclass
from typing import Optional

@dataclass
class Building:
    id: str
    name: str
    building_type: str
    floor_area_m2: float
    annual_energy_kwh: float
    eui_kwh_m2: Optional[float] = None
    energy_rating: Optional[str] = None

    def calculate_eui(self):
        self.eui_kwh_m2 = self.annual_energy_kwh / self.floor_area_m2
        return self.eui_kwh_m2

    def assign_rating(self):
        if self.eui_kwh_m2 is None:
            self.calculate_eui()
        if self.eui_kwh_m2 < 100:
            self.energy_rating = 'A'
        elif self.eui_kwh_m2 < 150:
            self.energy_rating = 'B'
        elif self.eui_kwh_m2 < 200:
            self.energy_rating = 'C'
        elif self.eui_kwh_m2 < 250:
            self.energy_rating = 'D'
        else:
            self.energy_rating = 'E'
        return self.energy_rating
