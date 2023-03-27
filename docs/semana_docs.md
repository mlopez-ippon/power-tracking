{% docs _surrogate_key %}
Unique identifier for the two id columns  
**Type** : string
{% enddocs %}

----- Bookings -----

{% docs booking_id %}
Unique identifier for the booking  
**Type** : number
{% enddocs %}

{% docs reservation_date %}
Date when the booking takes place  
**Type** : string
{% enddocs %}

{% docs period %}
Period of the day for when the booking is done
**Type** : string  
{% enddocs %}

{% docs created_at_France_bookings %}
Time when the booking has been realized  
**Type** : timestamp  
{% enddocs %}

{% docs type_res %}
What way to work
**Type** : string  
{% enddocs %}

----- Practices -----

{% docs community_id %}
Unique identifier for the practice  
**Type** : number
{% enddocs %}

{% docs practice_name %}
Name of the practice  
**Type** : string
{% enddocs %}

{% docs city_parent_id %}
ID of the city where the practice is
**Type** : number  
{% enddocs %}

{% docs parent_practice_name %}
Name of the parent practice if is not an agency 
**Type** : string
{% enddocs %}

{% docs nb_collaborators %}
Current number of collaborator per practice 
**Type** : string
{% enddocs %}

{% docs created_at_France_practices %}
Time when the practice has been created on Semana  
**Type** : timestamp  
{% enddocs %}

----- Collaborators -----

{% docs collaborator_id %}
Unique identifier for the collaborator  
**Type** : number
{% enddocs %}

{% docs role %}
Role of the collaborator on Semana  
**Type** : string
{% enddocs %}

{% docs status %}
Status of the collaborator on Semana  
**Type** : string
{% enddocs %}

{% docs created_at_France_collaborators %}
Time when the collaborator has been created on Semana 
**Type** : timestamp  
{% enddocs %}
