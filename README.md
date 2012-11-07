# SMOBS

SMOBS (Simple Mongo OBjectS) is a minimal, lightweight library for mapping MongoDB Documents to self-sufficient Python objects.
import smobs

## Examples

    # Setup DB connnection
    smobs.init('TEST')
    
    class Car(smobs.Document):
        """
        Car object.
        """
        
        def __after_init__(self):
            # Set some defaults
            if not hasattr(self, 'num_wheels'):
                self.num_wheels = 4
                
    car1 = Car(make='Nissan', colour='Black')
    car1.save()
    
    car2 = Car(make='Toyota', colour='Red')
    car2.save()
    
    car3 = Car(make='Reliant', colour='Blue', num_wheels=3)
    car3.save()
    
    print 'There are {0} cars.'.format(Car.count())
    
There are 3 cars.
    
    print '{0} of them have 4 wheels.'.format(Car.count({'num_wheels' : 4}))
    
2 of them have 4 wheels.
    
    print 'The Toyota is {0}.'.format(Car.find({'make' : 'Toyota'})[0].colour)
    
The Toyota is Red.
    
    for c in Car.all():
        c.delete()