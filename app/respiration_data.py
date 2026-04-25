RESPIRATION_DB = {
    "meta": {
        "temps_C": [0, 5, 10, 15, 20, 25],
        "units": "mg CO2 / kg / h",
        "source": "USDA respiration table transcription from provided images",
        "notes": {
            "nd": None,
            "lt_prefix": "value reported as an upper bound, e.g. '<10'"
        }
    },
    "data": {
        "apple_fall": {
            0: 3, 5: 6, 10: 9, 15: 15, 20: 20, 25: None
        },
        "apple_summer": {
            0: 5, 5: 8, 10: 17, 15: 25, 20: 31, 25: None
        },
        "avocado": {
            0: None, 5: 35, 10: 105, 15: None, 20: 190, 25: None
        },
        "blackberry": {
            0: 19, 5: 36, 10: 62, 15: 75, 20: 115, 25: None
        },
        "blueberry": {
            0: 6, 5: 11, 10: 29, 15: 48, 20: 70, 25: 101
        },
        "grape_american": {
            0: 3, 5: 6, 10: 8, 15: 16, 20: 33, 25: 39
        },
        "grape_muscadine": {
            0: 10, 5: 13, 10: None, 15: None, 20: 51, 25: None
        },
        "grape_table": {
            0: 3, 5: 7, 10: 13, 15: None, 20: 27, 25: None
        },
        "grapefruit": {
            0: None, 5: None, 10: None, 15: None, 20: None, 25: None
        },
        "mandarin_tangerine": {
            0: None, 5: 6, 10: 8, 15: 16, 20: 25, 25: None
        },
        "mango": {
            0: None, 5: 16, 10: 35, 15: 58, 20: 113, 25: None
        },
        "nectarine_ripe": {
            0: 5, 5: None, 10: 20, 15: None, 20: 87, 25: None
        },
        "papaya_ripe": {
            0: None, 5: 5, 10: None, 15: 19, 20: 80, 25: None
        },
        "passion_fruit": {
            0: None, 5: 44, 10: 59, 15: 141, 20: 262, 25: None
        },
        "peach_ripe": {
            0: 5, 5: None, 10: 20, 15: None, 20: 87, 25: None
        },
        "strawberry": {
            0: 16, 5: None, 10: 75, 15: None, 20: 150, 25: None
        },
        "watermelon": {
            0: None, 5: 4, 10: 8, 15: None, 20: 21, 25: None
        },
        "pomegranate": {
            0: None, 5: 6, 10: 12, 15: None, 20: 24, 25: None
        },
    },
    "annotations": {
        "grapefruit": {
            15: {
                "kind": "upper_bound",
                "value": 10,
                "raw": "<10"
            }
        }
    }
}
