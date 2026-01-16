variable "project_name" {
  type        = string
  description = "The base name for all resources (e.g., melpark)"
}

variable "location" {
  type        = string
  description = "The Azure region where resources will be created"
  default     = "Australia East"
}

variable "tags" {
  type        = map(string)
  description = "A map of tags to add to all resources"
  default     = {
    Environment = "Dev"
    Project     = "Melbourne Parking"
  }
}