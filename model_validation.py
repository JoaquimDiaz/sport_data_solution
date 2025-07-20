import logging
from datetime import datetime, timedelta
from typing import Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

ALLOWED_SPORT_TYPES = {
    "Voile", "Triathlon", "Runing", "Judo", "Randonnée", 
    "Tennis", "Tennis de table", "Football", "Badminton", 
    "Natation", "Boxe", "Rugby", "Escalade", "Basketball", "Équitation"
}


class StravaEvent(BaseModel):
    """Pydantic model for validating sport activity data from strava. """
    
    id: UUID = Field(..., description="Unique identifier for the activity")
    id_salarie: Union[str, int] = Field(..., description="Employee ID")
    date_debut: datetime = Field(..., description="Activity start datetime")
    duration: int = Field(..., gt=0, description="Duration in minutes")
    sport_type: str = Field(..., min_length=1, description="Type of sport activity")
    distance: Optional[int] = Field(None, ge=0, description="Distance in meters")
    comment: Optional[str] = Field(None, description="Optional comment")
    
    @field_validator('date_debut')
    @classmethod
    def validate_date_debut(cls, v):
        """Ensure date_debut is not in the future (with some tolerance)."""
        now = datetime.now()
        if v > now + timedelta(hours=1): 
            raise ValueError("Activity start date cannot be more than 1 hour in the future")
        return v
    
    @field_validator('duration')
    @classmethod
    def validate_duration(cls, v):
        """Ensure duration is reasonable (not more than 24 hours)."""
        if v > 1440:  # 24 hours in minutes
            raise ValueError("Duration cannot exceed 24 hours (1440 minutes)")
        return v
    
    @field_validator('sport_type')
    @classmethod
    def validate_sport_type(cls, v):
        """Normalize sport type and validate against allowed types."""
        if v not in ALLOWED_SPORT_TYPES:
            logger.warning(f"Unknown sport type: {v}")
        return v
    
    def calculate_end_time(self) -> datetime:
        """Calculate the end time of the activity."""
        return self.date_debut + timedelta(minutes=self.duration)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }